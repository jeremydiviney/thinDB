// mysql2 3.16.0 streams still decode values. Benchmark the complete wire
// response by bypassing its per-row parser while retaining protocol checks.
function drainQuery(connection, sql, values = {}) {
  return new Promise((resolve, reject) => {
    const sets = [];
    const command = connection.query(sql, values, (error) => {
      if (error) return reject(error);
      if (command._rowParser !== null) return reject(new Error('A row parser was created in discard mode'));
      const resultSets = sets.filter(Boolean);
      const last = resultSets.at(-1);
      resolve({
        length: last?.rows ?? 0,
        payloadBytes: last?.payloadBytes ?? 0,
        columns: last?.columns ?? 0,
        resultSets,
        decodedRows: 0,
        discarded: true,
      });
    });
    const original = Object.getPrototypeOf(command);
    command.readField = function () {
      if (this._receivedFieldsCount === 0) sets[this._resultIndex] = { columns: this._fieldCount, rows: 0, payloadBytes: 0 };
      this._receivedFieldsCount++;
      return this._receivedFieldsCount === this._fieldCount ? original.fieldsEOF : this.readField;
    };
    command.row = function (packet, conn) {
      // Preserve EOF status flags and additional result sets; data packets
      // must never reach the original row parser or accumulate in its arrays.
      if (packet.isEOF()) return original.row.call(this, packet, conn);
      const set = sets[this._resultIndex];
      set.rows++;
      set.payloadBytes += packet.length() - 4;
      return this.row;
    };
  });
}

module.exports = { drainQuery };
