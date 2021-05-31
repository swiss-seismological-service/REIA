import React from 'react';
import 'whatwg-fetch';
import ExposureUpload from './ExposureUpload';
import ExposureGrid from './ExposureGrid';
import { getExposure } from '../util/api';

class App extends React.Component {
    state = {
        exposureModels: null,
    };

    componentDidMount = () => {
        this.updateExposureModels();
    };

    updateExposureModels = () => {
        getExposure().then((response) => {
            this.setState({ exposureModels: response });
        });
    };

    render() {
        return (
            <>
                <ExposureUpload />
                <ExposureGrid data={this.state.exposureModels} />
            </>
        );
    }
}

export default App;
