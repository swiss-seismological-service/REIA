import React, { useState } from 'react';
import { Button, Grid, Paper, Typography, TextField } from '@material-ui/core';

import { postLossModel } from '../util/api';
import FileUpload from '../util/fileUpload';

const initial = {
    modelJson: null,
    assetCollectionId: '',
    vulnerabilityModelIds: '',
};

export default function LossModelUpload(props) {
    const [values, setValues] = useState(initial);

    const handleSubmission = () => {
        props.reload({ lossModels: null, lossModelLoading: true });
        postLossModel(values).then((response) => {
            props.reload({ lossModels: response, lossModelLoading: false });
        });
    };

    const handleChange = (e) => {
        const { name, value } = e.target;
        setValues({
            ...values,
            [name]: value,
        });
    };

    const handleFiles = (e) => {
        const { name, files } = e.target;
        setValues({
            ...values,
            [name]: files,
        });
    };

    return (
        <Paper className="paper">
            <Typography gutterBottom variant="h5" component="h2">
                Loss Model
            </Typography>
            <Grid container spacing={3} className="grid">
                <Grid item xs={2}>
                    <FileUpload currentFile={values.modelJson} setFile={handleFiles} name="modelJson">
                        Loss json
                    </FileUpload>
                </Grid>
                <Grid item xs={2}>
                    <TextField
                        id="ac-input"
                        label="Asset Collection ID"
                        name="assetCollectionId"
                        value={values.assetCollectionId}
                        onChange={handleChange}
                    />
                </Grid>
                <Grid item xs={2}>
                    <TextField
                        id="vm-input"
                        label="Vulnerability Models IDs"
                        name="vulnerabilityModelIds"
                        value={values.vulnerabilityModelIds}
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
