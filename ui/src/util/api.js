export async function getUsers(endpoint) {
    const response = fetch(endpoint)
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
