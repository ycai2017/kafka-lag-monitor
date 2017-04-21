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
 import styles from './Overview.css';

 class Overview extends Component {

   constructor(props) {
		super(props);
		this.state = {count: 0};
	 }

   componentDidMount() {
    fetch('http://localhost:9090/api/topics')
            .then(result=> {
                return result.json();
            }).then(data=> {
              this.setState({count:data.length})
            });
	 }

   render() {
     return (
       <div className="con">
         <div className="row">
          <div className="headers col-sm-3">
            Nodes: <span className="count">{this.state.count}</span>
          </div>
          <div className="headers col-sm-3">
            Topics: <span className="count">{this.state.count}</span>
          </div>
          <div className="headers col-sm-3">
            Partitions: <span className="count">{this.state.count}</span>
          </div>
          <div className="headers col-sm-3">
            Replication: <span className="count">{this.state.count}</span>
          </div>
         </div>
         <hr className="rule"/>
         <div>
         testing
         </div>
       </div>
     );
   }

 }

export default Overview
