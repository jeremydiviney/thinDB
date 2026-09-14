const fs = require('node:fs');
const {createRequire} = require('node:module');
const root = '/home/ubuntu/wayroll-bench';
const {drain_query} = require(root+'/ordinary_sql_diagnosis.cjs');
const mysql = createRequire(fs.realpathSync(root+'/five-arm-d29f5f1-sierra-dop12/app')+'/package.json')('mysql2');
async function main() {
  const [output,label,count='5'] = process.argv.slice(2);
  const source = JSON.parse(fs.readFileSync(root+'/ordinary-sql-diagnosis/timing-12-'+label+'-thin/query.json'));
  const connection=mysql.createConnection({host:'127.0.0.1',port:13311,user:'root',password:'',database:'wayroll_prod__public',namedPlaceholders:true,multipleStatements:true});
  const samples=[];
  try {
    for(let iteration=0;iteration<=Number(count)+1;iteration++) {
      const offset=fs.statSync(output+'/server.err').size;
      const validation=iteration===Number(count)+1;
      const result=await drain_query(connection,source.sql,source.params,validation);
      samples.push({iteration,validation,...result});
      fs.writeFileSync(`${output}/${label}-${iteration}.trace`,fs.readFileSync(output+'/server.err').subarray(offset));
      fs.writeFileSync(`${output}/${label}.json`,JSON.stringify({label,samples},null,2));
    }
    const prior=JSON.parse(fs.readFileSync(root+'/ordinary-sql-diagnosis/timing-12-'+label+'-thin/result.json')).samples.find(s=>s.validation);
    const result=samples.at(-1);
    if(result.rows!==prior.rows||result.columns!==prior.columns||result.digest!==prior.digest)throw Error('Fingerprint mismatch '+label);
    console.log(JSON.stringify({label,times:samples.filter(s=>s.iteration>0&&!s.validation).map(s=>s.wire_ms),rows:result.rows,fingerprint:true}));
  }finally{await connection.promise().end();}
}
main().catch(e=>{console.error(e.message);process.exitCode=1;});
