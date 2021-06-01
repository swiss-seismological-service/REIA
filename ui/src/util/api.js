export async function getExposure() {
    const response = fetch('/exposure')
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

export async function getVulnerability() {
    const response = fetch('/vulnerability')
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function postVulnerability(files) {
    const formData = new FormData();
    formData.append('vulnerabilityModel', files.selectedVulnerabilityModel);

    const response = fetch('/vulnerability', {
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
