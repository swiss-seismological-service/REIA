import React from 'react';
import 'whatwg-fetch';
import ExposureUpload from './ExposureUpload';

class App extends React.Component {
    state = {};

    ENDPOINT = '/test';

    componentDidMount = () => {
        console.log('hi');
    };

    render() {
        return (
            <>
                <ExposureUpload />
            </>
        );
    }
}

export default App;
