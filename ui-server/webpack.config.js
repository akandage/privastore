const dotenv = require('dotenv');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const webpack = require('webpack');
const path = require('path');

dotenv.config();
const port = process.env.SERVER_HTTP_PORT || '8080';

module.exports = {
    mode: 'development',
    entry: './src/index.tsx',
    output: {
        // path: path.join(__dirname, 'public', 'bundles'),
        filename: 'bundle.[hash].js'
    },
    devtool: 'inline-source-map',
    module: {
        rules: [
            {
                // Use babel loader to transform JSX to ES2015 javascript.
                test: /\.tsx$/,
                exclude: /node_modules/,
                use: 'ts-loader'
            },
            {
                test: /\.css$/,
                use: [
                  {
                    loader: 'style-loader'
                  },
                  {
                    loader: 'css-loader',
                    options: {
                        esModule: true,
                        modules: {
                          namedExport: true,
                        },
                        /* Don't resolve url() */
                        url: false
                    }
                  }
                ]
              },
              {
                test: /\.scss$/,
                use: [
                    {
                        loader: 'style-loader'
                    },
                    {
                        loader: 'css-loader',
                        options: {
                            esModule: true,
                            modules: {
                              namedExport: true,
                            }
                        }
                    },
                    {
                        loader: 'sass-loader',
                        options: {
                            implementation: require('sass')
                        }
                    }
                ]
              }
        ]
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js', '.jsx', '.css', '.scss'],
    },
    plugins: [
        new HtmlWebpackPlugin({
            template: 'public/index.html',
            // favicon: 'public/favicon.ico'
        })
    ],
    devServer: {
        host: 'localhost',
        port: port,
        historyApiFallback: true,
        open: true,
        static: [
            'public'
        ]
    }
};