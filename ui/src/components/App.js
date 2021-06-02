import React from 'react';
import 'whatwg-fetch';
import { DataGrid } from '@material-ui/data-grid';

import ExposureUpload from './ExposureUpload';
import VulnerabilityUpload from './VulnerabilityUpload';
import LossModelUpload from './LossModelUpload';
import { getExposure, getLossModel, getVulnerability } from '../util/api';

const columnsLossModel = [
    { field: 'id', headerName: 'ID', width: 50 },
    { field: 'description', headerName: 'Description', width: 300 },
    { field: 'preparationCalculationMode', headerName: 'Prep CM', width: 150 },
    { field: 'mainCalculationMode', headerName: 'Main CM', width: 150 },
    { field: 'numberOfGroundMotionFields', headerName: 'no. GMFs', type: 'number', width: 130 },
    { field: 'maximumDistance', headerName: 'Max. Distance', type: 'number', width: 150 },
    { field: 'randomSeed', headerName: 'Random Seed', type: 'number', width: 150 },
    { field: 'masterSeed', headerName: 'Master Seed', type: 'number', width: 150 },
    { field: 'truncationLevel', headerName: 'Truncation Lvl', type: 'number', width: 150 },
    { field: 'vulnerabilityModels', headerName: 'Vulnerability Models', width: 190 },
    { field: 'assetCollection', headerName: 'Asset Collection', type: 'number', width: 165 },
    { field: 'nCalculations', headerName: 'Loss Calculations', type: 'number', width: 165 },
];

const columnsVulnerability = [
    { field: 'id', headerName: 'ID', width: 50 },
    { field: 'lossCategory', headerName: 'lossCategory', width: 300 },
    { field: 'assetCategory', headerName: 'assetCategory', width: 150 },
    { field: 'description', headerName: 'description', width: 150 },
    { field: 'nFunctions', headerName: 'Functions', type: 'number', width: 120 },
];

const columnsExposure = [
    { field: 'id', headerName: 'ID', width: 50 },
    { field: 'name', headerName: 'Name', width: 300 },
    { field: 'category', headerName: 'Category', width: 150 },
    { field: 'taxonomySource', headerName: 'Taxonomy Source', width: 150 },
    { field: 'costTypes', headerName: 'Cost Types', width: 300 },
    { field: 'tagNames', headerName: 'Tag Names', width: 300 },
    { field: 'nAssets', headerName: 'Assets', type: 'number', width: 120 },
    { field: 'nSites', headerName: 'Sites', type: 'number', width: 120 },
];

class App extends React.Component {
    state = {
        exposureModels: null,
        exposureLoading: true,
        vulnerabilityModels: null,
        vulnerabilityLoading: true,
        lossModels: null,
        lossModelLoading: true,
    };

    componentDidMount = () => {
        this.getExposureModel();
        this.getVulnerabilityModel();
        this.getLossModel();
    };

    getExposureModel = () => {
        getExposure().then((response) => {
            this.setState({ exposureModels: response, exposureLoading: false });
        });
    };

    getVulnerabilityModel = () => {
        getVulnerability().then((response) => {
            this.setState({ vulnerabilityModels: response, vulnerabilityLoading: false });
        });
    };

    getLossModel = () => {
        getLossModel().then((response) => {
            this.setState({ lossModels: response, lossModelLoading: false });
        });
    };

    updateModelState = (newState) => {
        this.setState({ ...newState });
    };

    render() {
        return (
            <>
                <LossModelUpload reload={this.updateModelState} />
                <DataGrid
                    rows={this.state.lossModels || []}
                    columns={columnsLossModel}
                    pageSize={5}
                    loading={this.state.lossModelLoading}
                    disableColumnMenu
                    autoHeight
                />
                <ExposureUpload reload={this.updateModelState} />
                <div style={{ width: '100%' }}>
                    <DataGrid
                        rows={this.state.exposureModels || []}
                        columns={columnsExposure}
                        pageSize={5}
                        loading={this.state.exposureLoading}
                        disableColumnMenu
                        autoHeight
                    />
                </div>
                <VulnerabilityUpload reload={this.updateModelState} />
                <div style={{ width: '100%' }}>
                    <DataGrid
                        rows={this.state.vulnerabilityModels || []}
                        columns={columnsVulnerability}
                        pageSize={5}
                        loading={this.state.vulnerabilityLoading}
                        disableColumnMenu
                        autoHeight
                    />
                </div>
            </>
        );
    }
}

export default App;
