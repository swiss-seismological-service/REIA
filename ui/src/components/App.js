import React from 'react';
import 'whatwg-fetch';
import ExposureUpload from './ExposureUpload';
import ExposureGrid from './ExposureGrid';
import VulnerabilityGrid from './VulnerabilityGrid';
import VulnerabilityUpload from './VulnerabilityUpload';
import { getExposure, getVulnerability } from '../util/api';

class App extends React.Component {
    state = {
        exposureModels: null,
        exposureLoading: true,
        vulnerabilityModels: null,
        vulnerabilityLoading: true,
    };

    componentDidMount = () => {
        this.getExposureModels();
        this.getVulnerabilityModels();
    };

    getExposureModels = () => {
        getExposure().then((response) => {
            this.setState({ exposureModels: response, exposureLoading: false });
        });
    };

    setExposure = (newModel, loading) => {
        if (newModel) this.setState({ exposureModels: newModel });
        this.setState({ exposureLoading: loading });
    };

    getVulnerabilityModels = () => {
        getVulnerability().then((response) => {
            this.setState({ vulnerabilityModels: response, vulnerabilityLoading: false });
        });
    };

    setVulnerability = (newModel, loading) => {
        if (newModel) this.setState({ vulnerabilityModels: newModel });
        this.setState({ vulnerabilityLoading: loading });
    };

    render() {
        return (
            <>
                <ExposureUpload reload={this.setExposure} />
                <ExposureGrid data={this.state.exposureModels} loading={this.state.exposureLoading} />
                <VulnerabilityUpload reload={this.setVulnerability} />
                <VulnerabilityGrid data={this.state.vulnerabilityModels} loading={this.state.vulnerabilityLoading} />
            </>
        );
    }
}

export default App;
