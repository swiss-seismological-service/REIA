import React from 'react';
import { DataGrid } from '@material-ui/data-grid';

export default function ExposureGrid(props) {
    const columns = [
        { field: 'id', headerName: 'ID', width: 90 },
        { field: 'name', headerName: 'Name', width: 300 },
        { field: 'category', headerName: 'Category', width: 150 },
        { field: 'taxonomySource', headerName: 'Taxonomy Source', width: 150 },
        { field: 'costTypes', headerName: 'Cost Types', width: 300 },
        { field: 'tagNames', headerName: 'Tag Names', width: 300 },
        {
            field: 'nAssets',
            headerName: 'Assets',
            type: 'number',
            width: 120,
        },
        {
            field: 'nSites',
            headerName: 'Sites',
            type: 'number',
            width: 120,
        },
    ];

    return (
        <>
            {props.data ? (
                <div style={{ height: 400, width: '100%' }}>
                    <DataGrid rows={props.data} columns={columns} pageSize={5} disableColumnMenu />
                </div>
            ) : (
                ''
            )}
        </>
    );
}
