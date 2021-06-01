import React from 'react';
import 'whatwg-fetch';
import { Button, Grid, Paper, Typography } from '@material-ui/core';

import { postExposure } from '../util/api';
import FileUpload from '../util/fileUpload';

class ExposureUpload extends React.Component {
    state = {
        selectedExposureJSON: null,
        selectedExposureCSV: null,
    };

    handleSubmission = () => {
        this.props.reload(null, true);
        postExposure({ ...this.state }).then((response) => {
            this.props.reload(response, false);
        });
    };

    setSelectedExposureJSON = (file) => {
        this.setState({ selectedExposureJSON: file[0] });
    };

    setSelectedExposureCSV = (file) => {
        this.setState({ selectedExposureCSV: file[0] });
    };

    render() {
        return (
            <Paper className="paper">
                <Typography gutterBottom variant="h5" component="h2">
                    Exposure Model
                </Typography>
                <Grid container spacing={3} className="grid">
                    <Grid item xs={2}>
                        <FileUpload
                            currentFile={this.state.selectedExposureJSON}
                            setFile={this.setSelectedExposureJSON}
                            name="exposureJSON"
                        >
                            Exposure JSON
                        </FileUpload>
                    </Grid>
                    <Grid item xs={2}>
                        <FileUpload
                            currentFile={this.state.selectedExposureCSV}
                            setFile={this.setSelectedExposureCSV}
                            name="exposureCSV"
                        >
                            Exposure CSV
                        </FileUpload>
                    </Grid>
                    <Grid item xs={2}>
                        <Button variant="contained" color="secondary" onClick={this.handleSubmission}>
                            Submit
                        </Button>
                    </Grid>
                </Grid>
            </Paper>
        );
    }
}

export default ExposureUpload;
