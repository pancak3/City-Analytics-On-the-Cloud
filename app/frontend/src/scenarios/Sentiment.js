import React, { useEffect, useState } from 'react';
import PropTypes from 'prop-types';
import Scenario from '../components/Scenario';
import { getSentiment } from '../helper/api';
import { prepareGeoJSON } from './geo';
import {
    ExpansionPanel,
    ExpansionPanelSummary,
    ExpansionPanelDetails,
    ButtonGroup,
    Typography,
    Button,
    Grid,
    Table,
    TableRow,
    TableBody,
    TableCell,
} from '@material-ui/core';
import { Tooltip, PieChart, Pie, Cell } from 'recharts';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import LoadingBlock from '../components/LoadingBlock';

function Sentiment(props) {
    const plainGeo = props.geojson;

    const [geojson, setGeoJSON] = useState(null);
    const [overallLoaded, setOverallLoaded] = useState(false);
    const [overall, setOverall] = useState(null);
    const [marker, setMarker] = useState(null);
    const [settingsExpanded, setSettingsExpanded] = useState(true);
    const [areaInfoExpanded, setAreaInfoExpanded] = useState(true);

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
        setMarker(props.areaCentroid ? props.areaCentroid[feature_code] : null);
        setAreaInfoExpanded(true);
    }

    // Sentiment tweet info for area
    const barInfo = areaChosen
        ? [
              {
                  name: 'Positive',
                  value: overall.areas[areaChosen].positive || 0,
              },
              {
                  name: 'Negative',
                  value: overall.areas[areaChosen].negative || 0,
              },
              {
                  name: 'Neutral',
                  value: overall.areas[areaChosen].neutral || 0,
              },
          ]
        : [];
    const COLORS = ['#44b889', '#b84444', '#d1d1d1'];

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
                                <React.Fragment>
                                    <Typography className={'mt-4'}>
                                        <strong>
                                            IEO Pearson correlation coefficient:{' '}
                                        </strong>{' '}
                                        {overall ? (
                                            overall.ieo[0].substring(0, 8)
                                        ) : (
                                            <LoadingBlock>
                                                <span>0.123456</span>
                                            </LoadingBlock>
                                        )}
                                    </Typography>
                                    <Typography>
                                        <strong>p-value: </strong>
                                        {overall ? (
                                            overall.ieo[1].substring(0, 8)
                                        ) : (
                                            <LoadingBlock>
                                                <span>0.123456</span>
                                            </LoadingBlock>
                                        )}
                                    </Typography>
                                </React.Fragment>
                            ) : (
                                <React.Fragment>
                                    <Typography className={'mt-4'}>
                                        <strong>
                                            IEO Pearson correlation coefficient:{' '}
                                        </strong>{' '}
                                        {overall ? (
                                            overall.ier[0].substring(0, 8)
                                        ) : (
                                            <LoadingBlock>
                                                <span>0.123456</span>
                                            </LoadingBlock>
                                        )}
                                    </Typography>
                                    <Typography>
                                        <strong>p-value: </strong>
                                        {overall ? (
                                            overall.ier[1].substring(0, 8)
                                        ) : (
                                            <LoadingBlock>
                                                <span>0.123456</span>
                                            </LoadingBlock>
                                        )}
                                    </Typography>
                                </React.Fragment>
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
            {areaChosen ? (
                <div>
                    <ExpansionPanel
                        defaultExpanded
                        expanded={areaInfoExpanded}
                        onChange={(e) => setAreaInfoExpanded(!areaInfoExpanded)}
                    >
                        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                            <h5>Area information</h5>
                        </ExpansionPanelSummary>
                        <ExpansionPanelDetails>
                            <Grid>
                                <h6>
                                    {props.areaName
                                        ? props.areaName[areaChosen]
                                        : ''}{' '}
                                    ({areaChosen})
                                </h6>

                                <Table>
                                    <TableBody>
                                        <TableRow>
                                            <TableCell component="th">
                                                Population
                                            </TableCell>
                                            <TableCell>
                                                {datasetChosen === 'ieo'
                                                    ? overall.areas[areaChosen]
                                                          .ieo_pop
                                                    : overall.areas[areaChosen]
                                                          .ier_pop}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell component="th">
                                                {datasetChosen === 'ieo'
                                                    ? 'IEO score'
                                                    : 'IER score'}
                                            </TableCell>
                                            <TableCell>
                                                {datasetChosen === 'ieo'
                                                    ? overall.areas[areaChosen]
                                                          .ieo_score
                                                    : overall.areas[areaChosen]
                                                          .ier_score}
                                            </TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>

                                <PieChart
                                    className="pie"
                                    width={200}
                                    height={200}
                                >
                                    <Pie
                                        isAnimationActive={true}
                                        data={barInfo}
                                        outerRadius={80}
                                        label
                                        dataKey="value"
                                    >
                                        {barInfo.map((entry, index) => (
                                            <Cell
                                                key={`cell-${index}`}
                                                fill={
                                                    COLORS[
                                                        index % COLORS.length
                                                    ]
                                                }
                                            />
                                        ))}
                                    </Pie>
                                    <Tooltip />
                                </PieChart>

                                <div id="label-container">
                                    <p id="pos">
                                        <span></span>Positive
                                    </p>
                                    <p id="neg">
                                        <span></span>Negative
                                    </p>
                                    <p id="neu">
                                        <span></span>Neutral
                                    </p>
                                </div>
                            </Grid>
                        </ExpansionPanelDetails>
                    </ExpansionPanel>
                </div>
            ) : (
                <React.Fragment />
            )}
        </Scenario>
    );
}

Scenario.propTypes = {
    geojson: PropTypes.array,
    areaName: PropTypes.object,
    areaCentroid: PropTypes.object,
};

export default Sentiment;
