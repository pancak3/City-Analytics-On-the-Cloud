import React, { useEffect, useState } from 'react';
import Scenario from '../components/Scenario';
import { getCounts, getKeyword } from '../helper/api';
import PropTypes from 'prop-types';
import {
    ExpansionPanel,
    ExpansionPanelSummary,
    ExpansionPanelDetails,
    Input,
    Typography,
    Button,
    Grid,
    Box,
    TableHead,
    TableBody,
    TableContainer,
    TableCell,
    TableRow,
    Table,
} from '@material-ui/core';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';

// Adds values to geojson
const prepareGeoJSON = (geojson, data) => {
    return geojson && data
        ? geojson.map((feature) => {
              return {
                  ...feature,
                  properties: {
                      ...feature.properties,
                      feature_value: data[feature.properties.feature_code],
                  },
              };
          })
        : null;
};

function Word(props) {
    const plainGeo = props.geojson;

    const [counts, setCounts] = useState(null);
    const [loaded, setLoaded] = useState(false);
    const [keyword, setKeyword] = useState(false);
    const [keywordData, setKeywordData] = useState(null);
    const [keywordLoaded, setKeywordLoaded] = useState(true);
    const [geojson, setGeoJSON] = useState(null);
    const [freq, setFreq] = useState(null);

    // Load counts
    useEffect(() => {
        if (loaded) return;
        setLoaded(true);
        getCounts().then((data) => {
            setCounts(data);
        });
    }, [loaded, plainGeo]);

    // Load keywords
    useEffect(() => {
        if (!keyword || keywordLoaded) return;
        setKeywordLoaded(true);
        getKeyword(keyword).then((data) => {
            setKeywordData(data);
        });
    }, [keyword, keywordLoaded, plainGeo]);

    // Which data should be used
    const data = keywordData ? keywordData : counts;

    // When geojson is loaded
    useEffect(() => {
        if (plainGeo && data) {
            setGeoJSON(prepareGeoJSON(plainGeo, data));
        }
    }, [plainGeo, data]);

    // When data is changed, get top 5 frequency
    useEffect(() => {
        if (!data) return;
        const freq = data
            ? Object.keys(data).sort((a, b) => data[b] - data[a])
            : null;
        setFreq(freq.slice(0, 5));
    }, [data]);

    // Query for keyword
    function submit_keyword() {
        if (keyword === '' || !keyword) {
            console.log('here');
            setKeywordLoaded(true);
            setKeywordData(null);
            setGeoJSON(prepareGeoJSON(plainGeo, counts));
        } else {
            // load keyword data
            setKeywordLoaded(false);
        }
    }

    return (
        <Scenario data={geojson} mode={'k'}>
            <div>
                <ExpansionPanel defaultExpanded>
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                        <h5>Keyword Search</h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
                        <Grid>
                            <Typography>
                                Search by keyword (SA2 areas)
                            </Typography>
                            <Grid>
                                <Box mt={1}>
                                    <Input
                                        margin="none"
                                        placeholder="Keyword"
                                        onChange={(e) => {
                                            setKeyword(
                                                e.target.value.toLowerCase()
                                            );
                                        }}
                                    ></Input>
                                    <Button
                                        variant="outlined"
                                        color="primary"
                                        onClick={() => submit_keyword()}
                                    >
                                        Search
                                    </Button>
                                </Box>
                            </Grid>
                        </Grid>
                    </ExpansionPanelDetails>
                </ExpansionPanel>
            </div>
            <div>
                <ExpansionPanel defaultExpanded>
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                        <h5>Frequency</h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
                        {freq ? (
                            freq.length > 0 ? (
                                <TableContainer>
                                    <Table>
                                        <TableHead>
                                            <TableRow>
                                                <TableCell component="th">
                                                    SA2 Area
                                                </TableCell>
                                                <TableCell component="th">
                                                    Count
                                                </TableCell>
                                            </TableRow>
                                        </TableHead>
                                        <TableBody>
                                            {freq.map((area_code) => (
                                                <TableRow key={area_code}>
                                                    <TableCell>
                                                        {props.areaName
                                                            ? props.areaName[
                                                                  area_code
                                                              ]
                                                            : ''}
                                                    </TableCell>
                                                    <TableCell>
                                                        {data[area_code]}
                                                    </TableCell>
                                                </TableRow>
                                            ))}
                                        </TableBody>
                                    </Table>
                                </TableContainer>
                            ) : (
                                <p>No data found...</p>
                            )
                        ) : (
                            <p>Loading...</p>
                        )}
                    </ExpansionPanelDetails>
                </ExpansionPanel>
            </div>
        </Scenario>
    );
}

Word.propTypes = {
    geojson: PropTypes.array,
    areaName: PropTypes.object,
};

export default Word;
