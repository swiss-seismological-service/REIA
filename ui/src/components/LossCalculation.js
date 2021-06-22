import React from 'react';
import { Button, Grid, Paper, Typography } from '@material-ui/core';

export default function LossCalculation() {
    const startCalculation = () => {
        const response = fetch('/api/v1/calculation/run', {
            method: 'POST',
            body: '{"shakemap":"model/shapefiles.zip"}',
            headers: {
                'Content-Type': 'application/json',
            },
        })
            .then((resp) => {
                if (!resp.ok) throw Error(resp.statusText);
                return resp.json();
            })
            .then((json) => json);
        return response;
    };

    return (
        <Paper className="paper">
            <Typography gutterBottom variant="h5" component="h2">
                LossCalculation
            </Typography>

            <Grid item xs={2}>
                <Button variant="contained" color="secondary" onClick={startCalculation}>
                    New Calculation
                </Button>
            </Grid>
        </Paper>
    );
}
