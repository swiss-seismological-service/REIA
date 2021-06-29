import React, { useState } from 'react';
import { Button, Grid, Paper, Typography } from '@material-ui/core';

import { postExposure } from '../util/api';
import FileUpload from '../util/fileUpload';

const initial = {
    exposureXML: null,
    exposureCSV: null,
};

export default function ExposureUpload(props) {
    const [values, setValues] = useState(initial);

    const handleSubmission = () => {
        props.reload({ exposureModels: null, exposureLoading: true });
        postExposure(values).then((response) => {
            props.reload({ exposureModels: response, exposureLoading: false });
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
                Exposure Model
            </Typography>
            <Grid container spacing={3} className="grid">
                <Grid item xs={2}>
                    <FileUpload currentFile={values.exposureJSON} setFile={handleFiles} name="exposureXML">
                        Exposure XML
                    </FileUpload>
                </Grid>
                <Grid item xs={2}>
                    <FileUpload currentFile={values.exposureCSV} setFile={handleFiles} name="exposureCSV">
                        Exposure CSV
                    </FileUpload>
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
