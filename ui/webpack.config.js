const path = require('path');

module.exports = {
    entry: {
        index: path.resolve(__dirname, './src/index.js'),
    },
    module: {
        rules: [
            {
                test: /\.(js|jsx)$/,
                exclude: /node_modules/,
                use: ['babel-loader'],
            },
            {
                test: /\.css$/,
                use: ['style-loader', 'css-loader'],
            },
        ],
    },
    resolve: {
        extensions: ['*', '.js', '.jsx'],
    },
    output: {
        path: path.resolve(__dirname, '../app/static/js'),
        filename: '[name].js',
    },
    // optimization: {
    //     splitChunks: { chunks: 'all' },
    // },
};
