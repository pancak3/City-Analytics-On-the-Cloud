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

function Word(props) {
    const [counts, setCounts] = useState(null);
    const [loaded, setLoaded] = useState(false);
    const [keyword, setKeyword] = useState(false);
    const [keywordData, setKeywordData] = useState(null);
    const [keywordLoaded, setKeywordLoaded] = useState(true);

    useEffect(() => {
        if (loaded) return;

        setLoaded(true);
        getCounts().then((data) => {
            setCounts(data);
        });
    }, [loaded]);

    useEffect(() => {
        if (!keyword || keywordLoaded) return;
        setKeywordLoaded(true);
        getKeyword(keyword).then((data) => {
            setKeywordData(data);
        });
    }, [keyword, keywordLoaded]);

    // Which data should be used
    const data = keywordData ? keywordData : counts;

    // Put values into geojson
    const geojson =
        props.geojson && data
            ? props.geojson.map((feature) => {
                  return {
                      ...feature,
                      properties: {
                          ...feature.properties,
                          feature_value: data[feature.properties.feature_code],
                      },
                  };
              })
            : null;

    // Get top 5 frequency
    const freq = data
        ? Object.keys(data).sort((a, b) => data[b] - data[a])
        : null;

    // Query for keyword
    function submit_keyword() {
        if (keyword === '') {
            setKeywordLoaded(true);
            setKeywordData(null);
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
                                        {freq.slice(0, 5).map((area_code) => (
                                            <TableRow key={area_code}>
                                                <TableCell>
                                                    {props.areaName ? props.areaName[area_code] : ''}
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
