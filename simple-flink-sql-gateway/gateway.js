const express = require('express');
const { exec } = require("child_process");
const uuid = require("uuid");
const fs = require('fs');
const app = express();
const logger = require("./lib/logger.js");
const port = process.env.SIMPLE_FLINK_SQL_GATEWAY_PORT || 9000;
const flink_root = process.env.SIMPLE_FLINK_SQL_GATEWAY_ROOT || "./flink-1.13.0";
const sql_plugins = process.env.SIMPLE_FLINK_SQL_GATEWAY_ROOT || './plugins';


app.use(express.json());

app.post('/v1/sessions/:session_id/statements', function (request, response) {
  //for now ignore session_id
  var body = request.body;
  if (body.statement === undefined) {
    response.status(599);
    response.send("No statement field in body");
    return;
  }
  var id = uuid.v4();
  var filename = "/tmp/script_" + id + ".sql";
  fs.writeFileSync(filename, body.statement.toString());
  logger.info("Wrote " + filename + " with content " + body.statement);
  var command = flink_root + "/bin/sql-client.sh -l " + sql_plugins + "-f " + filename;
  //exec('')

  response.send('OK');
});



app.listen(port, function () {
  console.log('Listening on port ' + port);
});