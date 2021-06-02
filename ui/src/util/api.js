export async function getData(endpoint) {
    const response = fetch(endpoint)
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function postExposure(files) {
    const formData = new FormData();
    formData.append('exposureJSON', files.exposureJSON[0]);
    formData.append('exposureCSV', files.exposureCSV[0]);

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

export async function postVulnerability(files) {
    const formData = new FormData();
    formData.append('vulnerabilityModel', files[0]);

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

export async function postLossModel(values) {
    const formData = new FormData();
    formData.append('lossModel', values.modelJson[0]);
    formData.append('assetCollection', values.assetCollectionId);
    formData.append('vulnerabilityModels', values.vulnerabilityModelIds);
    const response = fetch('/lossmodel', {
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

export async function postLossConfig(data) {
    const response = fetch('/lossconfig', {
        method: 'POST',
        body: JSON.stringify(data),
    })
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}
