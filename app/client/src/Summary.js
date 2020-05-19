import React, { useEffect, useState } from 'react';
import Card from './Card';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faUser } from '@fortawesome/free-solid-svg-icons';
import { faTwitter } from '@fortawesome/free-brands-svg-icons';
import LoadingBlock from './LoadingBlock';
import { getStats } from './api';

function Summary() {
    const [loadRequired, setLoadRequired] = useState(true);
    const [stats, setStats] = useState(null);
    const [lastUpdated, setLastUpdated] = useState(null);

    useEffect(() => {
        if (!loadRequired) return;
        setLoadRequired(false);

        getStats().then((stats) => {
            setStats(stats);
        });
    }, [loadRequired]);

    return (
        <React.Fragment>
            <div className='container-fluid'>
                <h2>Summary</h2>
                <div className='row justify-content-start'>
                    <div className='col-3'>
                        <Card className='card-disp-num card-green'>
                            <h3>Users</h3>
                            { stats ? <p>{stats.user}</p> :
                                <LoadingBlock>
                                    <p>12,345</p>
                                </LoadingBlock>
                            }
                            <FontAwesomeIcon icon={faUser} />
                        </Card>
                    </div>
                    <div className='col-3'>
                        <Card className='card-disp-num card-blue'>
                            <h3>Tweets</h3>
                            { stats ? <p>{stats.status}</p> :
                                <LoadingBlock>
                                    <p>1,234,567</p>
                                </LoadingBlock>
                            }
                            <FontAwesomeIcon icon={faTwitter} />
                        </Card>
                    </div>
                </div>

                <p>
                    Horizontal graphs of day per week + hours per day
                </p>

            </div>

        </React.Fragment>
    );
}

export default Summary;
