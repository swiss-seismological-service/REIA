import React from 'react';
import 'whatwg-fetch';
import {Button} from '@material-ui/core';
import { getUsers } from '../util/api';
import FileUpload from './FileUpload';

class App extends React.Component {
    state = {
        name: 'unknown',
        data: null,
    };

    ENDPOINT = '/test'

    componentDidMount = () => {
        console.log('hi');
    };

    myFunction = () => {
        getUsers(this.ENDPOINT).then((json) => this.setState({ name: json[0].name, data: json }));
    };

    render() {
        const { name, data } = this.state;
        return (
            <>
            <div>
                <h1>Hi {name}</h1>
                Hello World
                <br />
                <Button variant="contained" color="primary" onClick={this.myFunction}>
                    Click Me
                </Button>
                {data ? data.map((person) => <p>{person.name}</p>) : ''}
            </div>
            <FileUpload></FileUpload>
            </>
        );
    }
}

export default App;
