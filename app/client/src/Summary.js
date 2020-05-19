import React from 'react';
import Card from './Card';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faUser } from '@fortawesome/free-solid-svg-icons';
import { faTwitter } from '@fortawesome/free-brands-svg-icons';

class Summary extends React.Component {
    render() {
        return (
            <React.Fragment>
                <div className='container-fluid'>
                    <h2>Summary</h2>
                    <div className='row justify-content-start'>
                        <div className='col-3'>
                            <Card className='card-disp-num card-green'>
                                <h3>Users</h3>
                                <p>12345</p>
                                <FontAwesomeIcon icon={faUser} />
                            </Card>
                        </div>
                        <div className='col-3'>
                            <Card className='card-disp-num card-blue'>
                                <h3>Tweets</h3>
                                <p>12345</p>
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
}

export default Summary;
