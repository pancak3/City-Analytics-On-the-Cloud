import React, { useEffect, useState } from 'react';
import Card from '../components/Card';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faUser } from '@fortawesome/free-solid-svg-icons';
import { faTwitter } from '@fortawesome/free-brands-svg-icons';
import LoadingBlock from '../components/LoadingBlock';
import { getStats, getGeneralInfo } from '../helper/api';
import Moment from 'react-moment';
import {
    BarChart,
    CartesianGrid,
    Bar,
    Tooltip,
    XAxis,
    YAxis,
    ResponsiveContainer,
    LineChart,
    Line,
} from 'recharts';

function Summary() {
    const [loadRequired, setLoadRequired] = useState(true);
    const [stats, setStats] = useState(null);
    const [info, setInfo] = useState(null);
    const [lastUpdated, setLastUpdated] = useState(null);

    useEffect(() => {
        if (!loadRequired) return;
        setLoadRequired(false);

        getStats().then((stats) => {
            setStats(stats);
        });

        getGeneralInfo().then((info) => {
            setInfo(info);
            setLastUpdated(new Date());
        });
    }, [loadRequired]);

    return (
        <React.Fragment>
            <div id="summary" className="container-fluid">
                <h2 className="mt-2">Summary</h2>
                <div className="row justify-content-start">
                    <div className="col-3">
                        <Card className="card-disp-num card-green">
                            <h3>Users</h3>
                            {stats ? (
                                <p>{stats.user}</p>
                            ) : (
                                <LoadingBlock>
                                    <p>12,345</p>
                                </LoadingBlock>
                            )}
                            <FontAwesomeIcon icon={faUser} />
                        </Card>
                    </div>
                    <div className="col-3">
                        <Card className="card-disp-num card-blue">
                            <h3>Tweets</h3>
                            {stats ? (
                                <p>{stats.status}</p>
                            ) : (
                                <LoadingBlock>
                                    <p>1,234,567</p>
                                </LoadingBlock>
                            )}
                            <FontAwesomeIcon icon={faTwitter} />
                        </Card>
                    </div>
                </div>

                <div>
                    <h3 className="mb-4">General statistics</h3>
                    <div id="front-table">
                        <div>
                            {info ? (
                                <React.Fragment>
                                    <h4 className="text-center mb-3">
                                        Tweets per hour of day
                                    </h4>
                                    <ResponsiveContainer
                                        width="100%"
                                        aspect={4.0 / 3.0}
                                    >
                                        <LineChart data={info.hours}>
                                            <CartesianGrid strokeDasharray="1 1" />
                                            <XAxis
                                                minTickGap={0}
                                                dataKey="key"
                                            />
                                            <YAxis></YAxis>
                                            <Tooltip />
                                            <Line
                                                type="monotone"
                                                dataKey="value"
                                                name="Number of tweets"
                                                stroke="#2F6493"
                                            />
                                        </LineChart>
                                    </ResponsiveContainer>
                                </React.Fragment>
                            ) : (
                                <React.Fragment></React.Fragment>
                            )}
                        </div>
                        <div>
                            {info ? (
                                <React.Fragment>
                                    <h4 className="text-center mb-3">
                                        Tweets per day of week
                                    </h4>
                                    <ResponsiveContainer
                                        width="100%"
                                        aspect={4.0 / 3.0}
                                    >
                                        <BarChart data={info.weekday}>
                                            <CartesianGrid strokeDasharray="1 1" />
                                            <XAxis
                                                minTickGap={1}
                                                dataKey="key"
                                            />
                                            <YAxis></YAxis>
                                            <Tooltip />
                                            <Bar
                                                dataKey="value"
                                                name="Number of tweets"
                                                fill="#2F6493"
                                            />
                                        </BarChart>
                                    </ResponsiveContainer>
                                </React.Fragment>
                            ) : (
                                <React.Fragment></React.Fragment>
                            )}
                        </div>
                        {/* <div id="table-left">Left table</div> */}
                    </div>
                </div>
            </div>
            <footer className="text-right">
                Last updated:{' '}
                {lastUpdated ? (
                    <Moment format="HH:mm">{lastUpdated}</Moment>
                ) : (
                    'Never'
                )}
            </footer>
        </React.Fragment>
    );
}

export default Summary;
