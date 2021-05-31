export async function getExposure() {
    const response = fetch('/exposure')
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function uploadFile(files, data, endpoint) {
    const formData = new FormData();

    [...files].forEach((file, index) => {
        formData.append(`file${index}`, file);
    });

    formData.append('data', data);

    const response = fetch(endpoint, {
        method: 'POST',
        body: formData,
    })
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function postExposure(files) {
    const formData = new FormData();
    formData.append('exposureJSON', files.selectedExposureJSON);
    formData.append('exposureCSV', files.selectedExposureCSV);

    const response = fetch('/exposure', {
        method: 'POST',
        body: formData,
    })
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}
