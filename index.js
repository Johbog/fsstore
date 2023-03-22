'use strict';
const { mkdir, stat, readdir, readFile, writeFile } = require('fs').promises;
const { resolve } = require('path');
const { randomUUID } = require('crypto');

class Store {
  constructor(name, data, index, dir) {
    this._name = name;
    this._data = data || [];
    this._index = index || {};

    this.dir = dir;
  }

  get(id) {
    return new Promise(resolve => {
      const i = this._index[id];
      if (i > -1) {
        resolve(this._data[i]);
      }
    });
  }

  getAll() {
    return new Promise(resolve => {
      resolve(this._data);
    });
  }

  async create(data) {
    const id = await new Promise(resolve => {
      const _data = Object.assign({}, data);
      if (!_data.id) {
        _data.id = randomUUID();
      }
      this._data.push(_data);
      this._index[_data.id] = this._data.length - 1;
      resolve(_data.id);
    });

    this.save();

    return { created: [ id ] };
  }

  async set(id, data) {
    await new Promise(resolve => {
      const i = this._index[id];
      if (i < 0) {
        throw `Could not find item with id "${id}"`;
      }
      this._data[i] = Object.assign({}, data);
      resolve();
    });

    this.save();

    return { updated: [ id ] };
  }

  async save() {
    const dataFile = resolve(this.dir, 'rows');
    const indexFile = resolve(this.dir, 'index');

    await mkdir(this.dir, { recursive: true });
    // TODO: cleanup file
    await Promise.all([
      writeFile(dataFile, this._data.map(r => JSON.stringify(r)).join('\n')),
      writeFile(indexFile, JSON.stringify(this._index)),
    ]);
  }

  rebuildIndex() {
    // TODO:
  }
}

class Storage {
  constructor(name, options) {
    this._options = Object.assign({ schemas: {} }, options);
    this._dir = resolve(__dirname, '.dbs', name);
    this._stores = {};
  }

  async load() {
    const stats = await stat(this._dir).catch(error => {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    });

    if (!stats) {
      await mkdir(this._dir, { recursive: true });
    }

    const stores = await readdir(this._dir);

    const promises = stores.map(async store => {
      const dir = resolve(this._dir, store);
      const stats = await stat(dir);
      if (!stats.isDirectory()) {
        return;
      }

      const dataFile = resolve(dir, 'rows');
      const indexFile = resolve(dir, 'index');
      const [ data, index ] = await Promise.all([
        readFile(dataFile, 'utf8').then(str => str.split('\n').flatMap(r => (r ? JSON.parse(r) : []))),
        readFile(indexFile, 'utf8').then(str => (str ? JSON.parse(str) : {})),
      ]);
      this._stores[store] = new Store(store, data, index, dir);
    });

    await Promise.all(promises);
  }

  store(name) {
    if (!this._stores[name]) {
      const dir = resolve(this._dir, name);
      this._stores[name] = new Store(name, [], {}, dir);
    }

    return this._stores[name];
  }
}

(async() => {
  try {
    const schemas = {
      contacts: {
        indices: [
          {
            name: 'fullname',
            keyPath: [ 'name.first', 'name.middle', 'name.last' ],
            unique: false,
            multiEntry: false,
            locale: null, // TODO
          },
        ],
        properties: {
          name: {
            type: 'object',
            properties: {
              first: {
                type: 'string',
                required: true,
              },
              middle: {
                type: 'string',
              },
              last: {
                type: 'string',
                required: true,
              },
            },
          },
        },
      },
    };

    const db = new Storage('test', { schemas });
    await db.load();

    const result = await db.store('contacts').get('2b1cc7d3-6be5-442a-81e5-537d0c5d7606');
    console.log(result);

    process.exit();
  }
  catch (error) {
    console.error(error);
    process.exit(1);
  }
})();
