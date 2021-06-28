/* eslint-disable no-underscore-dangle */
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
    { field: '_oid', headerName: 'ID', width: 50 },
    { field: 'losscategory', headerName: 'Loss Category', width: 200 },
    { field: 'aggregateby', headerName: 'Aggregate By', width: 300 },
    { field: '_lossmodel_oid', headerName: 'Loss Model', width: 200 },
];

const columnsLossModel = [
    { field: '_oid', headerName: 'ID', width: 50 },
    { field: 'description', headerName: 'Description', width: 300 },
    { field: 'preparationcalculationmode', headerName: 'Prep CM', width: 150 },
    { field: 'maincalculationmode', headerName: 'Main CM', width: 150 },
    { field: 'numberofgroundmotionfields', headerName: 'no. GMFs', type: 'number', width: 130 },
    { field: 'maximumdistance', headerName: 'Max. Distance', type: 'number', width: 150 },
    { field: 'randomseed', headerName: 'Random Seed', type: 'number', width: 150 },
    { field: 'masterseed', headerName: 'Master Seed', type: 'number', width: 150 },
    { field: 'truncationlevel', headerName: 'Truncation Lvl', type: 'number', width: 150 },
    { field: '_vulnerabilitymodels_oids', headerName: 'Vulnerability Models', width: 190 },
    { field: '_assetcollection_oid', headerName: 'Asset Collection', type: 'number', width: 165 },
    { field: 'calculations_count', headerName: 'Loss Calculations', type: 'number', width: 165 },
];

const columnsVulnerability = [
    { field: '_oid', headerName: 'ID', width: 50 },
    { field: 'losscategory', headerName: 'Loss Category', width: 300 },
    { field: 'assetcategory', headerName: 'Asset Category', width: 150 },
    { field: 'description', headerName: 'description', width: 150 },
    { field: 'functions_count', headerName: 'Functions', type: 'number', width: 120 },
];

const columnsExposure = [
    { field: '_oid', headerName: 'ID', width: 50 },
    { field: 'name', headerName: 'Name', width: 300 },
    { field: 'category', headerName: 'Category', width: 150 },
    { field: 'taxonomysource', headerName: 'Taxonomy Source', width: 150 },
    { field: 'costtypes', headerName: 'Cost Types', width: 300 },
    { field: 'tagnames', headerName: 'Tag Names', width: 300 },
    { field: 'assets_count', headerName: 'Assets', type: 'number', width: 120 },
    { field: 'sites_count', headerName: 'Sites', type: 'number', width: 120 },
];

const columnsCalculation = [
    { field: '_oid', headerName: 'ID', width: 50 },
    { field: 'losscategory', headerName: 'Category', width: 300 },
    { field: '_lossmodel_oid', headerName: 'Loss Model', width: 150 },
    { field: 'aggregateby', headerName: 'Aggregated By', width: 150 },
    { field: 'timestamp_starttime', headerName: 'Time', width: 300 },
];

class App extends React.Component {
    state = {
        exposureModels: null,
        exposureLoading: true,
        vulnerabilityModels: null,
        vulnerabilityLoading: true,
        lossmodels: null,
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
        getData('/lossmodel').then((res) => this.setState({ lossmodels: res, lossModelLoading: false }));
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
                    getRowId={(row) => row._oid}
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
                    getRowId={(row) => row._oid}
                    pageSize={5}
                    loading={this.state.lossConfigLoading}
                    disableColumnMenu
                    autoHeight
                />
                <LossModelUpload reload={this.updateModelState} />
                <DataGrid
                    rows={this.state.lossmodels || []}
                    columns={columnsLossModel}
                    getRowId={(row) => row._oid}
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
                        getRowId={(row) => row._oid}
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
                        getRowId={(row) => row._oid}
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
