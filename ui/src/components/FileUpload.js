import React, {useState} from 'react';
import 'whatwg-fetch';
import {Button} from '@material-ui/core';
import {uploadFile} from '../util/api';

function FileUpload(){
	const [selectedFile, setSelectedFile] = useState();
	const [isFilePicked, setIsFilePicked] = useState(false);

	const changeHandler = (event) => {
		setSelectedFile(event.target.files);
		setIsFilePicked(true);
	};

	const handleSubmission = () => {
        uploadFile(selectedFile, 'data', '/exposures')
	};

	return(
   <div>
			<input type="file" name="file0" onChange={changeHandler} />
			{isFilePicked ? (
				<div>
					<p>Filename: {selectedFile[0].name}</p>
					<p>Filetype: {selectedFile[0].type}</p>
					<p>Size in bytes: {selectedFile[0].size}</p>
					<p>
						lastModifiedDate:{' '}
						{selectedFile[0].lastModifiedDate.toLocaleDateString()}
					</p>
				</div>
			) : (
				<p>Select a file to show details</p>
			)}
			<div>
				<button onClick={handleSubmission}>Submit</button>
			</div>
		</div>
	)
}

export default FileUpload;
