import React from 'react';
import 'whatwg-fetch';
import { DataGrid } from '@material-ui/data-grid';

import ExposureUpload from './ExposureUpload';
import VulnerabilityUpload from './VulnerabilityUpload';
import LossModelUpload from './LossModelUpload';
import LossConfigUpload from './LossConfigUpload';
import LossCalculation from './LossCalculation';
import { getData } from '../util/api';

const columnsLossConfig = [
    { field: 'id', headerName: 'ID', width: 50 },
    { field: 'lossCategory', headerName: 'Loss Category', width: 200 },
    { field: 'aggregateBy', headerName: 'Aggregate By', width: 300 },
    { field: 'lossModel', headerName: 'Loss Model', width: 200 },
];

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
    { field: 'lossCategory', headerName: 'Loss Category', width: 300 },
    { field: 'assetCategory', headerName: 'Asset Category', width: 150 },
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

const columnsCalculation = [
    { field: 'id', headerName: 'ID', width: 50 },
    { field: 'lossCategory', headerName: 'Category', width: 300 },
    { field: 'lossModelId', headerName: 'Loss Model', width: 150 },
    { field: 'aggregateBy', headerName: 'Aggregated By', width: 150 },
    { field: 'timestamp', headerName: 'Time', width: 300 },
];

class App extends React.Component {
    state = {
        exposureModels: null,
        exposureLoading: true,
        vulnerabilityModels: null,
        vulnerabilityLoading: true,
        lossModels: null,
        lossModelLoading: true,
        lossConfigs: null,
        lossConfigLoading: true,
        lossCalculation: null,
        lossCalculationLoading: true,
    };

    componentDidMount = () => {
        getData('/exposure').then((res) => this.setState({ exposureModels: res, exposureLoading: false }));
        getData('/vulnerability').then((res) =>
            this.setState({ vulnerabilityModels: res, vulnerabilityLoading: false })
        );
        getData('/lossmodel').then((res) => this.setState({ lossModels: res, lossModelLoading: false }));
        getData('/lossconfig').then((res) => this.setState({ lossConfigs: res, lossConfigLoading: false }));
        getData('/losscalculation').then((res) =>
            this.setState({ lossCalculation: res, lossCalculationLoading: false })
        );
    };

    updateModelState = (newState) => {
        this.setState({ ...newState });
    };

    render() {
        return (
            <>
                <LossCalculation reload={this.updateModelState} />
                <DataGrid
                    rows={this.state.lossCalculation || []}
                    columns={columnsCalculation}
                    pageSize={5}
                    loading={this.state.lossCalculationLoading}
                    disableColumnMenu
                    autoHeight
                    onRowClick={(param) => {
                        window.open(`/plotlymap/${param.row.id}`, '_blank');
                    }}
                />
                <LossConfigUpload reload={this.updateModelState} />
                <DataGrid
                    rows={this.state.lossConfigs || []}
                    columns={columnsLossConfig}
                    pageSize={5}
                    loading={this.state.lossConfigLoading}
                    disableColumnMenu
                    autoHeight
                />
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
