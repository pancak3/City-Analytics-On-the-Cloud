import React, { useEffect, useState } from 'react';
import PropTypes from 'prop-types';
import Scenario from '../components/Scenario';
import { getSentiment, getSentimentArea } from '../helper/api';
import { prepareGeoJSON } from './geo';
import {
    ExpansionPanel,
    ExpansionPanelSummary,
    ExpansionPanelDetails,
    ButtonGroup,
    Typography,
    Button,
    Grid,
    Box,
} from '@material-ui/core';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';

function Sentiment(props) {
    const plainGeo = props.geojson;

    const [geojson, setGeoJSON] = useState(null);
    const [overallLoaded, setOverallLoaded] = useState(false);
    const [overall, setOverall] = useState(null);
    const [marker, setMarker] = useState(null);
    const [settingsExpanded, setSettingsExpanded] = useState(true);

    const [datasetChosen, setDatasetChosen] = useState('ieo');
    const [areaChosen, setAreaChosen] = useState(null);

    useEffect(() => {
        if (overallLoaded) return;
        setOverallLoaded(true);
        getSentiment().then((data) => {
            setOverall(data);
        });
    }, [overallLoaded]);

    // When data or geojson is loaded
    useEffect(() => {
        if (plainGeo && overall) {
            const data_copy = { ...overall.areas };

            if (datasetChosen === 'ieo') {
                for (const area of Object.keys(data_copy)) {
                    data_copy[area] = data_copy[area].ieo_normalised;
                }
                setGeoJSON(prepareGeoJSON(plainGeo, data_copy));
            } else if (datasetChosen === 'ier') {
                for (const area of Object.keys(data_copy)) {
                    data_copy[area] = data_copy[area].ier_normalised;
                }
                setGeoJSON(prepareGeoJSON(plainGeo, data_copy));
            }
        }
    }, [plainGeo, overall, datasetChosen]);

    // Get information about area
    function getArea(feature_code) {
        setAreaChosen(feature_code);
        setMarker(
            props.areaCentroid ? props.areaCentroid[feature_code] : null
        );
    }

    return (
        <Scenario
            mode={'e'}
            data={geojson}
            featureClick={(feature) => {
                getArea(feature.properties.feature_code);
            }}
            marker={marker}
        >
            <div>
                <ExpansionPanel
                    defaultExpanded
                    expanded={settingsExpanded}
                    onChange={(e) => setSettingsExpanded(!settingsExpanded)}
                >
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                        <h5>AURIN Data</h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
                        <Grid>
                            <ButtonGroup
                                color="primary"
                                aria-label="outlined primary button group"
                            >
                                <Button
                                    onClick={(e) => setDatasetChosen('ieo')}
                                >
                                    IEO
                                </Button>
                                <Button
                                    onClick={(e) => setDatasetChosen('ier')}
                                >
                                    IER
                                </Button>
                            </ButtonGroup>

                            {datasetChosen === 'ieo' ? (
                                <Typography className="mt-4">
                                    <strong>IEO correlation: </strong> 0
                                </Typography>
                            ) : (
                                <Typography className="mt-4">
                                    <strong>IER correlation: </strong> 0
                                </Typography>
                            )}

                            {datasetChosen === 'ieo' ? (
                                <Typography>Description about IEO</Typography>
                            ) : (
                                <Typography>Description about IER</Typography>
                            )}
                        </Grid>

                        {/* <ToggleButtonGroup
                            value={datasetChosen}
                            onChange={handleDatasetChosen}
                        >
                            <ToggleButton value="ieo" aria-label="ieo">
                                IEO
                            </ToggleButton>
                            <ToggleButton value="ier" aria-label="ier">
                                IER
                            </ToggleButton>
                        </ToggleButtonGroup> */}
                    </ExpansionPanelDetails>
                </ExpansionPanel>
            </div>
        </Scenario>
    );
}

Scenario.propTypes = {
    geojson: PropTypes.array,
    areaName: PropTypes.object,
    areaCentroid: PropTypes.object,
};

export default Sentiment;
