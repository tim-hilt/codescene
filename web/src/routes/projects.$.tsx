import Card from "@/components/Card";
import PackedCircles from "@/components/PackedCircles";
import PlotFigure from "@/components/PlotFigure";
import * as Plot from "@observablehq/plot";
import { createFileRoute } from "@tanstack/react-router";
import * as d3 from "d3";

type CommitData = {
	commitDate: Date;
	sloc: number;
	complexity: number;
};

type ContributorData = {
	contributor: string;
	commits: number;
};

type CommitFrequency = {
	day: Date;
	commits: number;
};

type ProjectMetadata = {
	commitData: Array<CommitData>;
	contributorData: Array<ContributorData>;
	commitFrequency: Array<CommitFrequency>;
};

export const Route = createFileRoute("/projects/$")({
	component: RouteComponent,
	// TODO: Possibly, the whole loader thing is not the correct way to go and I should use different suspense boundary restricted components with react query and useSuspenseQueries?
	loader: async ({ params }): Promise<ProjectMetadata> => {
		const response = await fetch(
			`http://localhost:8000/projects/${params._splat}/metadata`,
		);
		return response.json(); // TODO: Handle not found error correctly
	},
});

function RouteComponent() {
	const params = Route.useParams();
	const data = Route.useLoaderData();

	data.commitData = data.commitData.map((cd) => ({
		...cd,
		commitDate: new Date(cd.commitDate),
	}));
	data.commitFrequency = data.commitFrequency.map((cf) => ({
		...cf,
		day: new Date(cf.day),
	}));

	const contributorData = data.contributorData.slice(undefined, 30);

	const oneYearAgo = new Date();
	oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
	oneYearAgo.setHours(0, 0, 0, 0);

	let commitFrequencyStartIndex = data.commitFrequency.findIndex(
		(cf) => cf.day.getTime() >= oneYearAgo.getTime(),
	);
	if (commitFrequencyStartIndex === -1) {
		commitFrequencyStartIndex = 0;
	}
	const commitFrequency = data.commitFrequency.slice(commitFrequencyStartIndex);

	/**
	 * TODO: Hotspots (Zoomable bubble chart)
	 * TODO: Re-Analyze when going to project
	 */

	return (
		<div className="flex flex-col space-y-12">
			<h1 className="text-4xl">{params._splat}</h1>
			<div className="grid grid-cols-4 gap-4">
				<Card title="Total Commits" content={data.commitData.length} />
				<Card
					title="Commits Last Year"
					content={
						data.commitData.filter(
							(cd) => cd.commitDate.getTime() >= oneYearAgo.getTime(),
						).length
					}
				/>
				<Card
					title="Newest Commit At"
					content={data.commitData[
						data.commitData.length - 1
					].commitDate.toLocaleString()}
				/>
				<Card title="Contributors" content={data.contributorData.length} />
			</div>
			<PackedCircles />
			<div className="grid grid-cols-2 gap-8">
				<PlotFigure
					options={{
						title: `Commits Of Top ${contributorData.length} Contributors`,
						height: 400,
						marginBottom: 130,
						x: { tickRotate: 45, labelOffset: 120 },
						marks: [
							Plot.barY(contributorData, {
								x: "contributor",
								y: "commits",
								sort: { x: "y", reverse: true },
								tip: true,
							}),
						],
					}}
				/>
				<PlotFigure
					options={{
						title: "Commits Over Time",
						height: 300,
						x: {
							label: "Commit Date",
						},
						y: {
							label: "Commit Number",
						},
						marks: [
							Plot.line(data.commitData, {
								x: "commitDate",
								y: (_, i) => i + 1,
								tip: true,
							}),
						],
					}}
				/>
			</div>
			<PlotFigure
				className="w-full"
				options={{
					marks: [
						Plot.line(data.commitData, {
							x: "commitDate",
							y: "complexity",
							stroke: "red",
							tip: true,
						}),
						Plot.line(data.commitData, {
							// TODO: Scale to have same range
							x: "commitDate",
							y: "sloc",
							stroke: "blue",
							tip: true,
						}),
						Plot.linearRegressionY(
							data.commitData.slice(
								data.commitData.length - Math.round(data.commitData.length / 8),
							),
							{
								x: "commitDate",
								y: "sloc",
								stroke: "orange",
							},
						),
					],
				}}
			/>
			{/* <PlotFigure
				options={{
					title: "Line Survival",
					color: {
						legend: true,
						label: "Year",
						tickFormat: "",
						type: "categorical",
					},
					x: {
						label: "Commit Date",
					},
					y: {
						label: "LOC",
					},
					marks: [
						Plot.areaY(data.lineSurvival, {
							x: "commitDate",
							y: "loc",
							fill: "year",
							tip: true,
						}),
					],
				}}
			/> */}
			<div className="mx-auto">
				<PlotFigure
					options={{
						title: "Commits Per Day During Last Year",
						padding: 0,
						width: 1200,
						x: {
							axis: false,
							label: "Calendarweek",
						},
						y: {
							tickFormat: Plot.formatWeekday("en", "short"),
							tickSize: 0,
							ticks: [1, 3, 5],
						},
						color: {
							label: "Frequency",
							legend: true,
							type: "quantize",
							interpolate: d3.interpolateGreens, // TODO: Special color, when 0, others should be more distinct, see github
						},
						marks: [
							Plot.cell(commitFrequency, {
								x: (d: CommitFrequency) =>
									d3.utcWeek.count(commitFrequency[0].day, d.day),
								y: (d: CommitFrequency) => d.day.getUTCDay(),
								fill: "commits",
								inset: 3,
								r: 3,
							}),
							Plot.text(
								d3.utcSunday.range(oneYearAgo, new Date()).filter((date) => {
									return (
										date.getUTCDate() <= 7 &&
										date.getUTCDate() <=
											new Date(
												date.getUTCFullYear(),
												date.getUTCMonth() + 1,
												0,
											).getUTCDate()
									);
								}),
								{
									x: {
										transform: (data: Date[]) => {
											const x = data.map((d) =>
												d3.utcWeek.count(commitFrequency[0].day, d),
											);
											return x;
										},
									},
									y: -1,
									frameAnchor: "middle",
									text: d3.utcFormat("%b"),
								},
							),
							Plot.tip(
								commitFrequency,
								Plot.pointer({
									x: (d: CommitFrequency) =>
										d3.utcWeek.count(commitFrequency[0].day, d.day),
									y: (d: CommitFrequency) => d.day.getUTCDay(),
									title: (d: CommitFrequency) => {
										const day = d.day.getDate();
										const month = d.day.toLocaleString("en-US", {
											month: "long",
										});

										const suffix =
											day % 10 === 1 && day !== 11
												? "st"
												: day % 10 === 2 && day !== 12
													? "nd"
													: day % 10 === 3 && day !== 13
														? "rd"
														: "th";
										const contributions = d.commits || "No";
										return `${contributions} contributions on ${month} ${day}${suffix}`;
									},
								}),
							),
						],
					}}
				/>
			</div>
		</div>
	);
}
