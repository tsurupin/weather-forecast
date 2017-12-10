const webpack = require('webpack')
const path = require('path');

const ROOT_PATH = path.resolve(__dirname);
const config = {
  entry: __dirname + '/js/index.js',
  output: {
    path: __dirname + '/dist',
    filename: 'bundle.js',
    publicPath: '/dist/'
  },
  module: {
    rules: [
      {
        test: /\.js?/,
        exclude: /node_modules/,
        use: 'babel-loader'
      }
    ]
  },
  resolve: {
    modules: [path.resolve(__dirname, '/static/js'), 'node_modules'],
    extensions: ['.js', '.jsx', '.css'],
    alias: {
      components: path.resolve(ROOT_PATH, 'static/js/components'),
      containers: path.resolve(ROOT_PATH, 'static/js/containers'),
      pages: path.resolve(ROOT_PATH, 'static/js/pages')
    }
  },
};

module.exports = config;

