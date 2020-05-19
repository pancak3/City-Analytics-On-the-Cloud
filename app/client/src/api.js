import axios from 'axios';
const BASE_URL = process.env.URL === undefined ? 'http://localhost:3000': '';

export function getStats() {
    return axios.get(`${BASE_URL}/api/stats`).then((response) => {
        return response.data;
    });
}

export function getGeneralInfo() {
    return axios.get(`${BASE_URL}/api/general`).then((response) => {
        return response.data;
    });
}
