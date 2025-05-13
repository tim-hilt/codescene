import Nav from "@/components/Nav";
import { Outlet, createRootRoute } from "@tanstack/react-router";
import { TanStackRouterDevtools } from "@tanstack/react-router-devtools";

export const Route = createRootRoute({
	component: () => (
		<div className="bg-neutral-100 min-h-dvh">
			<Nav />
			<div className="p-4">
				<Outlet />
			</div>
			<TanStackRouterDevtools />
		</div>
	),
});
