use crate::compare::{Cell, Output};
use async_trait::async_trait;
use mysql_async::{Conn, OptsBuilder, Row, Value, prelude::Queryable};
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug)]
pub struct WireError {
    pub state: Option<String>,
    pub message: String,
}
impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {}",
            self.state.as_deref().unwrap_or("transport"),
            self.message
        )
    }
}
impl std::error::Error for WireError {}
impl From<mysql_async::Error> for WireError {
    fn from(e: mysql_async::Error) -> Self {
        match e {
            mysql_async::Error::Server(e) => Self {
                state: Some(e.state),
                message: format!("{} {}", e.code, e.message),
            },
            e => Self {
                state: None,
                message: e.to_string(),
            },
        }
    }
}
impl From<tokio_postgres::Error> for WireError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self {
            state: e.code().map(|s| s.code().into()),
            message: e
                .as_db_error()
                .map(|s| s.message().to_string())
                .unwrap_or_else(|| e.to_string()),
        }
    }
}

pub enum Connection {
    Mysql(Conn),
    Postgres(tokio_postgres::Client),
}
pub struct Database {
    pub conn: Connection,
    pub last: Arc<Mutex<Option<Result<Output, WireError>>>>,
    pub describe_result: Arc<Mutex<bool>>,
}

impl Database {
    pub async fn connect(
        protocol: &str,
        port: u16,
        password: &str,
        database: &str,
    ) -> Result<Self, WireError> {
        let conn = if protocol == "mysql" {
            let opts = OptsBuilder::default()
                .ip_or_hostname("127.0.0.1")
                .tcp_port(port)
                .user(Some("root"))
                .pass(Some(password))
                .db_name(Some(database))
                .prefer_socket(false);
            Connection::Mysql(Conn::new(opts).await?)
        } else {
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(port)
                .user("root")
                .password(password)
                .dbname(database)
                .connect(tokio_postgres::NoTls)
                .await?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Connection::Postgres(client)
        };
        Ok(Self {
            conn,
            last: Arc::new(Mutex::new(None)),
            describe_result: Arc::new(Mutex::new(false)),
        })
    }
    pub async fn execute(&mut self, sql: &str) -> Result<Output, WireError> {
        self.execute_inner(sql, false).await
    }
    pub async fn query(&mut self, sql: &str) -> Result<Output, WireError> {
        self.execute_inner(sql, true).await
    }
    async fn execute_inner(&mut self, sql: &str, describe: bool) -> Result<Output, WireError> {
        match &mut self.conn {
            Connection::Mysql(conn) => {
                let mut result = conn.query_iter(sql).await?;
                let columns = result.columns_ref().to_vec();
                let affected = result.affected_rows();
                let rows: Vec<Row> = result.collect().await?;
                if !result.is_empty() {
                    return Err(WireError {
                        state: None,
                        message: "multiple result sets in one SQLLogicTest record".into(),
                    });
                }
                result.drop_result().await?;
                let mut output = Output {
                    columns: columns.len(),
                    rows: vec![],
                    affected,
                };
                for row in rows {
                    let mut cells = vec![];
                    for (i, column) in columns.iter().enumerate() {
                        let kind = column.column_type() as u8;
                        let value = match &row[i] {
                            Value::NULL => Cell::Null,
                            Value::Bytes(bytes) => {
                                let text =
                                    String::from_utf8(bytes.clone()).map_err(|e| WireError {
                                        state: None,
                                        message: e.to_string(),
                                    })?;
                                match kind {
                                    4 | 5 => Cell::Float(text),
                                    0 | 1 | 2 | 3 | 8 | 9 | 13 | 246 => Cell::Number(text),
                                    _ => Cell::Text(text),
                                }
                            }
                            other => {
                                return Err(WireError {
                                    state: None,
                                    message: format!("non-text MySQL result: {other:?}"),
                                });
                            }
                        };
                        cells.push(value);
                    }
                    output.rows.push(cells);
                }
                Ok(output)
            }
            Connection::Postgres(client) => {
                // Describe can execute INSERT SELECT in thinDB. Only query
                // records need metadata; statement records must run once.
                let statement = if describe {
                    Some(client.prepare(sql).await?)
                } else {
                    None
                };
                let columns = statement.as_ref().map(|s| s.columns()).unwrap_or(&[]);
                let mut output = Output {
                    columns: columns.len(),
                    ..Default::default()
                };
                for message in client.simple_query(sql).await? {
                    match message {
                        tokio_postgres::SimpleQueryMessage::Row(row) => {
                            if row.len() != columns.len() {
                                return Err(WireError {
                                    state: None,
                                    message:
                                        "PostgreSQL result width differs from Describe metadata"
                                            .into(),
                                });
                            }
                            let cells = (0..row.len())
                                .map(|i| match row.get(i) {
                                    None => Cell::Null,
                                    Some(s) => match columns[i].type_().name() {
                                        "int2" | "int4" | "int8" | "numeric" => {
                                            Cell::Number(s.into())
                                        }
                                        "float4" | "float8" => Cell::Float(s.into()),
                                        _ => Cell::Text(s.into()),
                                    },
                                })
                                .collect();
                            output.rows.push(cells);
                        }
                        tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                            output.affected = n
                        }
                        _ => {}
                    }
                }
                Ok(output)
            }
        }
    }
}

#[async_trait]
impl AsyncDB for Database {
    type Error = WireError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let describe = *self.describe_result.lock().unwrap();
        let result = self.execute_inner(sql, describe).await;
        *self.last.lock().unwrap() = Some(result.clone());
        let output = result?;
        if output.columns == 0 {
            Ok(DBOutput::StatementComplete(output.affected))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Any; output.columns],
                rows: output
                    .rows
                    .iter()
                    .map(|r| r.iter().map(Cell::slt).collect())
                    .collect(),
            })
        }
    }
    async fn shutdown(&mut self) {}
    fn engine_name(&self) -> &str {
        "thindb"
    }
    fn error_sql_state(error: &Self::Error) -> Option<String> {
        error.state.clone()
    }
}
