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
 import Topic from './Topic.js';

 class Overview extends Component {

   constructor(props) {
		super(props);
		this.state = {nodeCount: 0, topicCount: 0, throughputb: 0, throughputm: 0,replication:0, partitions:0 };
	 }

   componentDidMount() {
     fetch('http://localhost:9090/api/nodes')
             .then(result=> {
                 return result.json();
             }).then(data=> {
               this.setState({nodeCount:data})
             });
    fetch('http://localhost:9090/api/topics')
            .then(result=> {
                return result.json();
            }).then(data=> {
              this.setState({topicCount:data.length})
            });
            fetch('http://localhost:9090/api/throughputb')
                    .then(result=> {
                        return result.json();
                    }).then(data=> {
                      this.setState({throughputb:data})
                    });
                    fetch('http://localhost:9090/api/throughputm')
                            .then(result=> {
                                return result.json();
                            }).then(data=> {
                              this.setState({throughputm:data})
                            });
                    fetch('http://localhost:9090/api/replication')
                            .then(result=> {
                                return result.json();
                            }).then(data=> {
                              this.setState({replication:data})
                            });
                            fetch('http://localhost:9090/api/partitions')
                                    .then(result=> {
                                        return result.json();
                                    }).then(data=> {
                                      this.setState({partitions:data})
                                    });
	 }

   render() {
     return (
       <div className="con">
         <div className="row">
          <div className="headers col-sm-6">
            Nodes: <span className="count">{this.state.nodeCount}</span>
          </div>
          <div className="headers col-sm-6">
            Topics: <span className="count">{this.state.topicCount}</span>
          </div>
          </div>

          <div className="row">
          <div className="headers col-sm-6">
            Partitions: <span className="count">{this.state.partitions}</span>
          </div>
          <div className="headers col-sm-6">
            Replication: <span className="count">{this.state.replication}</span>
          </div>
          </div>

          <div className="row">
          <div className="headers col-sm-6">
            Throughput (Bytes): <span className="count">{this.state.throughputb}</span>
          </div>
          <div className="headers col-sm-6">
            Throughput (Messages): <span className="count">{this.state.throughputm}</span>
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
