import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/new")({
	component: RouteComponent,
});

// TODO: Add form to add new project
// TODO: Use this to show analysis-progress: https://observablehq.com/@d3/arc-tween
// TODO: Can we use websockets instead of SSE, in order to attach to analysis-progress?

function RouteComponent() {
	return <div>Hello "/new"!</div>;
}
