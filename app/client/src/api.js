import axios from 'axios';
const BASE_URL = process.env.URL || 'http://localhost:3000';

export function getStats() {
    return axios.get(`${BASE_URL}/api/stats`).then((response) => {
        return response.data;
    });
}
