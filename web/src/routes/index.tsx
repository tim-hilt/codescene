import { Link, createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/")({
	component: App,
	loader: async () => {
		const response = await fetch("http://localhost:8000/projects");
		return response.json();
	},
});

function App() {
	const data: Array<string> = Route.useLoaderData();
	return (
		<>
			<div className="mb-8 flex items-center justify-between">
				<h1 className="text-4xl">Projects</h1>
				<Link
					className="border rounded px-2 font-bold text-2xl cursor-pointer"
					to="/new"
				>
					+
				</Link>
			</div>
			<div className="grid grid-cols-4 gap-4">
				{data.map((d) => {
					const project = d.split("/").pop() || d;
					return (
						<Link
							className="bg-white shadow-lg rounded-lg h-[25vw] flex items-center justify-center text-2xl"
							key={project}
							to="/projects/$"
							params={{ _splat: d }}
						>
							{project}
						</Link>
					);
				})}
			</div>
		</>
	);
}
