import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { agentsQueryKey } from "../hooks/useAgentsQuery";
import { CreateProjectAgentSheet, RequiredAgentField } from "./CreateProjectAgentSheet";

function renderSheet(onSubmit = vi.fn().mockResolvedValue(undefined)) {
	const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
	queryClient.setQueryData(agentsQueryKey, {
		supported: [
			{ id: "codex", label: "codex" },
			{ id: "opencode", label: "opencode" },
		],
		installed: [
			{ id: "codex", label: "codex", authStatus: "authorized" },
			{ id: "opencode", label: "opencode", authStatus: "authorized" },
		],
		authorized: [
			{ id: "codex", label: "codex", authStatus: "authorized" },
			{ id: "opencode", label: "opencode", authStatus: "authorized" },
		],
	});
	render(
		<QueryClientProvider client={queryClient}>
			<CreateProjectAgentSheet
				isCreating={false}
				kind="single_repo"
				onOpenChange={() => undefined}
				onSubmit={onSubmit}
				open={true}
				path="/repo/new-project"
			/>
		</QueryClientProvider>,
	);
	return onSubmit;
}

async function chooseOption(trigger: HTMLElement, optionName: string) {
	await userEvent.click(trigger);
	await userEvent.click(await screen.findByRole("option", { name: optionName }));
}

describe("CreateProjectAgentSheet", () => {
	it("uses the compact trigger size for agent fields", () => {
		render(
			<RequiredAgentField
				id="agent"
				label="Agent"
				onChange={() => undefined}
				placeholder="Project default"
				value="claude-code"
				supported={[{ id: "codex", label: "Codex" }]}
			/>,
		);

		expect(screen.getByLabelText("Agent")).toHaveAttribute("data-size", "sm");
	});

	it("caps the agent menu height with a theme token", async () => {
		render(
			<RequiredAgentField
				id="agent"
				label="Agent"
				onChange={() => undefined}
				placeholder="Project default"
				value=""
				supported={[{ id: "codex", label: "Codex" }]}
				installed={[{ id: "codex", label: "Codex", authStatus: "authorized" }]}
				authorized={[{ id: "codex", label: "Codex", authStatus: "authorized" }]}
			/>,
		);

		await userEvent.click(screen.getByLabelText("Agent"));

		expect(await screen.findByRole("listbox")).toHaveClass("max-h-select-menu-max!");
	});

	it("does not fabricate selectable agents when the catalog omits them", async () => {
		render(
			<RequiredAgentField
				id="agent"
				label="Agent"
				onChange={() => undefined}
				placeholder="Project default"
				value="claude-code"
				supported={[{ id: "codex", label: "Codex" }]}
				installed={[{ id: "codex", label: "Codex", authStatus: "authorized" }]}
				authorized={[{ id: "codex", label: "Codex", authStatus: "authorized" }]}
			/>,
		);

		expect(screen.getByRole("combobox", { name: "Agent" })).toHaveTextContent("Project default");
		await userEvent.click(screen.getByRole("combobox", { name: "Agent" }));

		expect(await screen.findByRole("option", { name: "Codex" })).toBeInTheDocument();
		expect(screen.queryByRole("option", { name: /claude/i })).not.toBeInTheDocument();
	});

	it("creates without intake when the toggle is left off", async () => {
		const onSubmit = renderSheet();
		await chooseOption(screen.getByLabelText("Worker agent"), "codex");
		await chooseOption(screen.getByLabelText("Orchestrator agent"), "opencode");

		await userEvent.click(screen.getByRole("button", { name: "Create and start" }));

		await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
		expect(onSubmit).toHaveBeenCalledWith({
			workerAgent: "codex",
			orchestratorAgent: "opencode",
			trackerIntake: undefined,
		});
	});

	it("blocks submit when intake is enabled with no assignee, then passes the intake payload once one is set", async () => {
		const onSubmit = renderSheet();
		await chooseOption(screen.getByLabelText("Worker agent"), "codex");
		await chooseOption(screen.getByLabelText("Orchestrator agent"), "opencode");

		await userEvent.click(screen.getByLabelText("Enable issue intake"));
		// Enabled with no eligibility rule → submit stays disabled (compact sheet
		// carries no inline guard prose; gating is the disabled button).
		expect(screen.getByRole("button", { name: "Create and start" })).toBeDisabled();

		await userEvent.type(screen.getByLabelText("Assignee"), "octocat");
		await userEvent.click(screen.getByRole("button", { name: "Create and start" }));

		await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
		expect(onSubmit).toHaveBeenCalledWith({
			workerAgent: "codex",
			orchestratorAgent: "opencode",
			trackerIntake: { enabled: true, provider: "github", assignee: "octocat" },
		});
	});

	it("keeps the create sheet minimal: info tooltip instead of prose, no repo row or credential hint", async () => {
		renderSheet();
		// Info affordance is present even before enabling; the descriptive prose is not.
		expect(screen.getByLabelText("What does enabling issue intake do?")).toBeInTheDocument();
		expect(screen.queryByText(/Auto-spawn worker sessions from matching tracker issues/)).not.toBeInTheDocument();

		await userEvent.click(screen.getByLabelText("Enable issue intake"));
		expect(screen.queryByText("Repository")).not.toBeInTheDocument();
		expect(screen.queryByText(/Reads credentials from/)).not.toBeInTheDocument();
	});
});
