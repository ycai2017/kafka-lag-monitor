/**
 * Copyright 2017 Ambud Sharma
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import React, { Component } from 'react';
import Topic from './Topic';
import Overview from './Overview';

class App extends Component {
  render() {
    return (
      <div>
        <nav className="navbar navbar-inverse navbar-fixed-top">
      		<div className="container">
      			<div className="navbar-header">
      				<a className="navbar-brand" href="/"> <img alt="logo"
      					src="./styles/img/lag.png" style={{width: '20px', float: 'left'}} />
      					<p style={{float: 'left', marginTop: '2px', textDecoration: 'blink'}}>&nbsp;Kafka Lag
      						Monitor</p>
      				</a>
      			</div>
      		</div>
      	</nav>
      	<div className="container"
      		style={{paddingTop: '70px', margin: '0 auto', width: '80%'}}>
          <Overview /><br/>
        </div>
      </div>
    );
  }
}

export default App;
