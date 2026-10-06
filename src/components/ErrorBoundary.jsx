import React from "react";

// 單一頁面出錯時不讓整站白屏
export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }
  static getDerivedStateFromError(error) {
    return { error };
  }
  componentDidUpdate(prev) {
    if (prev.resetKey !== this.props.resetKey && this.state.error) this.setState({ error: null });
  }
  render() {
    if (!this.state.error) return this.props.children;
    return (
      <div className="page">
        <div className="notice err">
          這個畫面發生錯誤：{String(this.state.error.message || this.state.error)}
          <div style={{ marginTop: 8 }}>
            <button type="button" className="btn sm" onClick={() => this.setState({ error: null })}>重試</button>
          </div>
        </div>
      </div>
    );
  }
}
