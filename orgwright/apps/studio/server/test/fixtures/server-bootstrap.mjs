import { Server } from 'node:http';

// Use an OS-assigned loopback port without changing the production entry point.
const listen = Server.prototype.listen;
Server.prototype.listen = function (...args) {
  args[0] = 0;
  this.once('listening', () => process.send?.({ port: this.address().port }));
  return listen.apply(this, args);
};

await import('../../index.ts');
