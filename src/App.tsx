import React from "react";
import { OverlayView } from "./components/OverlayView";
import { MainView } from "./components/MainView";
import "./App.css";

export const App: React.FC = () => {
  const params = new URLSearchParams(window.location.search);
  const windowType = params.get("window") || "main";

  if (windowType === "overlay") {
    return <OverlayView />;
  }
  return <MainView />;
};

export default App;
