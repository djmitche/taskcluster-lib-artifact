const assume = require('assume');
const http = require('http');
const {Controller} = require('remotely-signed-s3');

/**
 * This is an object which pretends to be the queue.  NOTE: This queue implementation
 * only supports content type 'application/octet-stream'
 */
class Queue {
  constructor({bucket, port}) {
    this.bucket = bucket;
    this.port = port;

    this.controller = new Controller({region: 'us-west-2'});

    this.server = http.createServer((req, res) => {
      let loc = `https://${this.bucket}.s3-us-west-2.amazonaws.com/${this.key}`;
      res.writeHead(303, "See Other", {
        Location: loc,
      });
      res.end();
    });
  }

  async start() {
    return new Promise((res, rej) => {
      this.server.listen(this.port, err => {
        if (err) return rej(err);
        res();
      });
    })
  }

  async stop() {
    return new Promise((res, rej) => {
      this.server.close(err => {
        if (err) return rej(err);
        res();
      });
    });
  }

  async createArtifact(taskId, runId, name, body) {
    if (this.inprogress) {
      throw new Error('This mock only allows one in progress upload');
    }
    this.inprogress = true;
    assume(taskId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(body).to.be.an('object');
    let key = [taskId, runId, name].join('/');
    
    // We need to store the key because we'll respond with a redirect from the
    // server's request handler
    this.key = key;

    if (body.parts) {
      let uploadId = await this.controller.initiateMultipartUpload({
        bucket: this.bucket,
        key,
        sha256: body.contentSha256,
        size: body.contentLength,
        transferSha256: body.transferSha256,
        transferSize: body.transferLength,
        contentType: body.contentType,
        contentEncoding: body.contentEncoding
      });

      this.uploadId = uploadId;

      let requests = await this.controller.generateMultipartRequest({
        bucket: this.bucket,
        key,
        uploadId,
        parts: body.parts,
      });

      return {
        storageType: 'blob',
        expires: 'lala',
        requests
      };
    } else {
      let request = await this.controller.generateSinglepartRequest({
        bucket: this.bucket,
        key,
        sha256: body.contentSha256,
        size: body.contentLength,
        transferSha256: body.transferSha256,
        transferSize: body.transferLength,
        contentType: body.contentType,
        contentEncoding: body.contentEncoding
      });
      return {
        storageType: 'blob',
        expires: 'lala',
        requests: [request]
      };
    }
  }

  async completeArtifact(taskId, runId, name, body) {
    if (!this.inprogress) {
      throw new Error('Must have upload in progress to complete it');
    }
    delete this.inprogress;
    assume(taskId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(body).to.be.an('object');
    let key = [taskId, runId, name].join('/');
    if (this.uploadId) {
      await this.controller.completeMultipartUpload({
        bucket: this.bucket,
        key,
        etags: body.etags,
        uploadId: this.uploadId,
      });
    }
    this.inprogress = false;
  }

  buildUrl(method, taskId, runId, name) {
    return `http://localhost:30000`;
  } 
}


module.exports = {Queue};
