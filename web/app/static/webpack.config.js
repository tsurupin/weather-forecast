const webpack = require('webpack')
const path = require('path');

const stopUglifyJSWarnings = new webpack.optimize.UglifyJsPlugin({
  compress: {
    warnings: false
  }
});

const ROOT_PATH = path.resolve(__dirname);

const LAUNCH_COMMAND = process.env.npm_lifecycle_event;

const isProduction = LAUNCH_COMMAND === 'deploy';

process.env.BABEL_ENV = LAUNCH_COMMAND;

const productionPlugin = new webpack.DefinePlugin({
  'process.env': {
    NODE_ENV: JSON.stringify('production')
  }
});


const base = {
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
  devServer: {
    historyApiFallback: {
      index: '/index.html'
    }
  },
  plugins: [
    new webpack.LoaderOptionsPlugin({
      debug: true
    })
  ]
};

const developmentConfig = {
  devtool: 'cheap-module-eval-source-map',
  plugins: []
};

const productionConfig = {
  devtool: 'cheap-module-source-map',
  plugins: [productionPlugin, stopUglifyJSWarnings]
};

module.exports = Object.assign({}, base, isProduction ? productionConfig : developmentConfig );
