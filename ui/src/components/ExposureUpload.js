import React, { useState } from 'react';
import 'whatwg-fetch';
import { Button } from '@material-ui/core';
import { uploadFile } from '../util/api';

function ExposureUpload() {
    const [selectedExposureJSON, setselectedExposureJSON] = useState();
    const [selectedExposureCSV, setselectedExposureCSV] = useState();

    const handleSubmission = () => {
        uploadFile(selectedExposureJSON, 'data', '/exposures');
    };

    return (
        <div>
            <label htmlFor="contained-button-file">
                <input
                    style={{ display: 'none' }}
                    id="contained-button-file"
                    type="file"
                    onChange={(e) => setselectedExposureJSON(e.target.files)}
                    name="file"
                />
                <Button variant="contained" color="primary" component="span">
                    Upload Exposure JSON
                </Button>
            </label>
            {selectedExposureJSON ? (
                <p>Filename: {selectedExposureJSON[0].name}</p>
            ) : (
                <p>Select a file to show details</p>
            )}

            <label htmlFor="contained-button-file2">
                <input
                    style={{ display: 'none' }}
                    id="contained-button-file2"
                    type="file"
                    onChange={(e) => setselectedExposureCSV(e.target.files)}
                    name="file2"
                />
                <Button variant="contained" color="primary" component="span">
                    Upload Exposure CSV
                </Button>
            </label>
            {selectedExposureCSV ? (
                <p>Filename: {selectedExposureCSV[0].name}</p>
            ) : (
                <p>Select a file to show details</p>
            )}

            <div>
                <Button variant="contained" color="secondary" onClick={handleSubmission}>
                    Submit
                </Button>
            </div>
        </div>
    );
}

export default ExposureUpload;
