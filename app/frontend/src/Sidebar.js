import React from 'react';
import { NavLink } from 'react-router-dom';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faList, faBook, faSmile ,faRunning} from '@fortawesome/free-solid-svg-icons';

class Sidebar extends React.Component {
    render() {
        return (
            <aside>
                <h1>COMP90024 Assignment 2</h1>
                <ul>
                    <li>
                        <NavLink exact activeClassName="selected" to="/">
                            <FontAwesomeIcon icon={faList} />
                            Summary
                        </NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/scenario/word">
                            <FontAwesomeIcon icon={faBook} />
                            Keyword Search
                        </NavLink>
                    </li>
                    <li>
                        <NavLink
                            activeClassName="selected"
                            to="/scenario/sports-exercise"
                        >
                            <FontAwesomeIcon className="running" icon={faRunning} />
                            Sports and Exercise
                        </NavLink>
                    </li>
                    <li>
                        <NavLink
                            activeClassName="selected"
                            to="/scenario/sentiment"
                        >
                            <FontAwesomeIcon className="smile" icon={faSmile} />
                            Sentiment
                        </NavLink>
                    </li>

                </ul>
                <p>
                    Made by: Team 42
                    <br />
                    {'Data from '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://twitter.com/"
                    >
                        Twitter
                    </a>
                    {' and '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://aurin.org.au/"
                    >
                        AURIN
                    </a>
                    <br />
                    Design inspired by{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://covid-dashboards.web.app"
                    >
                        Covid 19 App
                    </a>
                </p>
            </aside>
        );
    }
}
export default Sidebar;
