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

 class Topic extends Component {

   constructor(props) {
		super(props);
		this.state = {topics: []};
	 }

   componentDidMount() {
    fetch('http://localhost:9090/api/topics')
            .then(result=> {
                return result.json();
            }).then(data=> {
              this.setState({topics:data})
            });
	 }

   render() {
     var content = [];
     for (var i in this.state.topics) {
        content.push(<option key={i}>{this.state.topics[i]}</option>);
     }
     return (
       <select className="selectpicker">
        {content}
       </select>
     );
   }

 }

export default Topic
