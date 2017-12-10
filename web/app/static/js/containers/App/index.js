import React, { Component } from "react";

class App extends Component {
  constructor(props) {
    super(props);
  }

  render () {
    return (
      <div>
        <Wrapper>
          {this.props.children}
        </Wrapper>
      </div>
    )
  }
}

export default App;

