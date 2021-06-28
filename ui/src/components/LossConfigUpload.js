import React, { useState } from 'react';
import { Button, Grid, Paper, Typography, TextField } from '@material-ui/core';

import { postLossConfig } from '../util/api';

const initial = {
    losscategory: '',
    aggregateby: '',
    _lossmodel_oid: '',
};

export default function LossConfigUpload(props) {
    const [values, setValues] = useState(initial);

    const handleSubmission = () => {
        props.reload({ lossConfigs: null, lossConfigLoading: true });
        postLossConfig(values).then((response) => {
            props.reload({ lossConfigs: response, lossConfigLoading: false });
        });
    };

    const handleChange = (e) => {
        const { name, value } = e.target;
        setValues({
            ...values,
            [name]: value,
        });
    };

    return (
        <Paper className="paper">
            <Typography gutterBottom variant="h5" component="h2">
                Loss Config
            </Typography>
            <Grid container spacing={3} className="grid">
                <Grid item xs={2}>
                    <TextField
                        id="lossCategory-input"
                        label="Loss Category"
                        name="losscategory"
                        value={values.lossCategory}
                        onChange={handleChange}
                    />
                </Grid>
                <Grid item xs={2}>
                    <TextField
                        id="aggregateBy-input"
                        label="aggregate by"
                        name="aggregateby"
                        value={values.aggregateBy}
                        onChange={handleChange}
                    />
                </Grid>
                <Grid item xs={2}>
                    <TextField
                        id="lossModel-input"
                        label="Loss Model ID"
                        name="_lossmodel_oid"
                        value={values.lossModelId}
                        onChange={handleChange}
                    />
                </Grid>
                <Grid item xs={2}>
                    <Button variant="contained" color="secondary" onClick={handleSubmission}>
                        Submit
                    </Button>
                </Grid>
            </Grid>
        </Paper>
    );
}
