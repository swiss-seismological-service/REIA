import React from 'react';
import { Button, Grid } from '@material-ui/core';

export default function FileUpload(props) {
    return (
        <Grid container spacing={0} className="grid">
            <Grid item xs={6}>
                <label htmlFor={`${props.name}-button`}>
                    <input
                        style={{ display: 'none' }}
                        id={`${props.name}-button`}
                        type="file"
                        onChange={(e) => props.setFile(e.target.files)}
                        name={props.name}
                    />
                    <Button variant="contained" color="primary" component="span">
                        {props.children}
                    </Button>
                </label>
            </Grid>
            <Grid item xs={6}>
                {props.currentFile ? <p>{props.currentFile.name.substring(0, 15)}...</p> : <p>Select a File...</p>}
            </Grid>
        </Grid>
    );
}
