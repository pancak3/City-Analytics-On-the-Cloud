import React from 'react';
import { NavLink } from 'react-router-dom';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faList, faRunning } from '@fortawesome/free-solid-svg-icons';

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
                        <NavLink
                            activeClassName="selected"
                            to="/scenario/exercise"
                        >
                            <FontAwesomeIcon icon={faRunning} />
                            Exercise
                        </NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/scenario/s2">
                            Scenario 2
                        </NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/scenario/s3">
                            Scenario 3
                        </NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/scenario/s4">
                            Scenario 4
                        </NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/scenario/s5">
                            Scenario 5
                        </NavLink>
                    </li>
                </ul>
                <p>
                    Made by: Team 42
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
