#pragma once

#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include "../stringtools.h"

namespace vss_settings
{
	// Bitmask constants matching UrBackup's c_use_group / c_use_value / c_use_value_client flags.
	constexpr int kUseGroup = 1;
	constexpr int kUseValue = 2;
	constexpr int kUseValueClient = 4;
	// Represents a single parsed writer/component selection entry from vss_select_components.
	struct VssComponentRule
	{
		std::string writer;
		std::string name;
		std::string path;

		bool operator==(const VssComponentRule& other) const
		{
			return strlower(writer) == strlower(other.writer)
				&& name == other.name
				&& path == other.path;
		}
	};

	// Structured representation of a vss_select_components configuration layer or resolved result.
	// Using a dedicated struct avoids fragile string concatenation and lets exclusions override inclusions.
	struct VssComponentSpec
	{
		bool has_all = false;
		bool all = false;
		bool has_default = false;
		bool select_default = false;
		std::vector<VssComponentRule> includes;
		std::set<std::string> excluded_tokens;
	};

	// Normalizes a VSS writer GUID or friendly alias (e.g. "exchange", "sql") to lowercase canonical GUIDs.
	// Returning a list allows aliases like "exchange" to cover both Exchange Store and Replica writers.
	inline std::vector<std::string> expandWriterAlias(const std::string& raw_token)
	{
		const std::string lower = strlower(trim(raw_token));
		std::vector<std::string> out;
		if (lower.empty())
		{
			return out;
		}
		if (lower == "exchange" || lower == "msexchange")
		{
			out.push_back("{76fe1ac4-15f7-4bcd-987e-8e1acb462fb7}"); // Microsoft Exchange Writer
			out.push_back("{7630e38d-08a7-490c-9d46-8bf0d80e04a9}"); // Microsoft Exchange Replica Writer
			return out;
		}
		if (lower == "sql" || lower == "mssql" || lower == "sqlserver")
		{
			out.push_back("{a65faa63-5ea8-4ebc-9dbd-a0c4db26912a}"); // Microsoft SQL Server Writer
			return out;
		}
		out.push_back(lower);
		return out;
	}

	// Checks whether a given writer GUID (or alias) is matched by the exclusion set.
	inline bool isWriterExcluded(const std::set<std::string>& excluded_tokens, const std::string& writer_guid_or_alias)
	{
		const std::vector<std::string> expanded = expandWriterAlias(writer_guid_or_alias);
		for (size_t i = 0; i < expanded.size(); ++i)
		{
			if (excluded_tokens.find(expanded[i]) != excluded_tokens.end())
			{
				return true;
			}
		}
		return false;
	}

	// Parses a '&'-delimited vss_select_components string into a structured VssComponentSpec.
	// Supports standard key=value pairs (all, default, writer_N, name_N, path_N) and negation prefixes ('-Exchange', '-{GUID}').
	inline VssComponentSpec parseVssComponentSpec(const std::string& raw)
	{
		VssComponentSpec spec;
		if (raw.empty())
		{
			return spec;
		}

		std::vector<std::string> parts;
		Tokenize(raw, parts, "&");

		// Collect indexed writer/name/path entries before materializing rules so key order does not matter.
		struct IndexedEntry
		{
			std::string writer;
			std::string name;
			std::string path;
		};
		std::map<std::string, IndexedEntry> indexed;

		for (size_t i = 0; i < parts.size(); ++i)
		{
			const std::string token = trim(parts[i]);
			if (token.empty())
			{
				continue;
			}

			const size_t eq_pos = token.find('=');
			if (eq_pos == std::string::npos)
			{
				// Standalone negation token such as "-Exchange" or "-{7630e38d-...}"
				if (token[0] == '-' && token.size() > 1)
				{
					const std::vector<std::string> expanded = expandWriterAlias(token.substr(1));
					spec.excluded_tokens.insert(expanded.begin(), expanded.end());
				}
				continue;
			}

			const std::string key = trim(token.substr(0, eq_pos));
			const std::string val = trim(token.substr(eq_pos + 1));

			if (key[0] == '-' && key.size() > 1)
			{
				// Key-value negation form such as "-writer={GUID}" or "-Exchange=1"
				const std::string target = (strlower(key) == "-writer" && !val.empty()) ? val : key.substr(1);
				if (val != "0")
				{
					const std::vector<std::string> expanded = expandWriterAlias(target);
					spec.excluded_tokens.insert(expanded.begin(), expanded.end());
				}
				continue;
			}

			if (key == "all")
			{
				spec.has_all = true;
				spec.all = (!val.empty() && val != "0" && strlower(val) != "false");
			}
			else if (key == "default")
			{
				spec.has_default = true;
				spec.select_default = (!val.empty() && val != "0" && strlower(val) != "false");
			}
			else if (next(key, 0, "writer_"))
			{
				indexed[getafter("writer_", key)].writer = val;
			}
			else if (next(key, 0, "name_"))
			{
				indexed[getafter("name_", key)].name = val;
			}
			else if (next(key, 0, "path_"))
			{
				indexed[getafter("path_", key)].path = val;
			}
			else if (next(key, 0, "exclude_") && !val.empty())
			{
				const std::vector<std::string> expanded = expandWriterAlias(val);
				spec.excluded_tokens.insert(expanded.begin(), expanded.end());
			}
		}

		for (std::map<std::string, IndexedEntry>::const_iterator it = indexed.begin();
			it != indexed.end(); ++it)
		{
			if (!it->second.writer.empty())
			{
				VssComponentRule rule;
				rule.writer = it->second.writer;
				rule.name = it->second.name;
				rule.path = it->second.path;
				spec.includes.push_back(rule);
			}
		}

		return spec;
	}

	// Serializes a resolved VssComponentSpec back into UrBackup's '&'-delimited wire/config format.
	inline std::string serializeVssComponentSpec(const VssComponentSpec& spec)
	{
		std::string out;
		auto append = [&out](const std::string& kv)
		{
			if (kv.empty())
			{
				return;
			}
			if (!out.empty())
			{
				out += "&";
			}
			out += kv;
		};

		if (spec.has_all)
		{
			append(std::string("all=") + (spec.all ? "1" : "0"));
		}
		if (spec.has_default)
		{
			append(std::string("default=") + (spec.select_default ? "1" : "0"));
		}

		for (size_t i = 0; i < spec.includes.size(); ++i)
		{
			const std::string idx = convert(i);
			append("writer_" + idx + "=" + spec.includes[i].writer);
			if (!spec.includes[i].name.empty())
			{
				append("name_" + idx + "=" + spec.includes[i].name);
			}
			if (!spec.includes[i].path.empty())
			{
				append("path_" + idx + "=" + spec.includes[i].path);
			}
		}

		for (std::set<std::string>::const_iterator it = spec.excluded_tokens.begin();
			it != spec.excluded_tokens.end(); ++it)
		{
			append("-" + *it);
		}

		return out;
	}

	// Computes the effective VSS component specification from group, server, and client layers.
	// Enforces allow_component_config, respects exclusive modes (configured here / on client / from group),
	// and resolves "use combination" by letting explicit exclusions and server-side default=0/all=0 constraints win.
	inline VssComponentSpec resolveVssComponentSpec(
		int use,
		bool allow_component_config,
		const std::string& group_val,
		const std::string& server_val,
		const std::string& client_val)
	{
		int effective_use = use;
		if (!allow_component_config)
		{
			// Discard client overrides whenever the server disallows client component configuration.
			effective_use &= ~kUseValueClient;
			if (effective_use == 0)
			{
				effective_use = !server_val.empty() ? kUseValue : kUseGroup;
			}
		}

		if (effective_use == kUseValue)
		{
			return parseVssComponentSpec(server_val);
		}
		if (effective_use == kUseValueClient)
		{
			return parseVssComponentSpec(client_val);
		}
		if (effective_use == kUseGroup || effective_use == 0)
		{
			return parseVssComponentSpec(group_val);
		}

		// "use combination": merge active layers with explicit precedence rules.
		std::vector<VssComponentSpec> higher_layers;
		if (effective_use & kUseGroup)
		{
			higher_layers.push_back(parseVssComponentSpec(group_val));
		}
		if (effective_use & kUseValue)
		{
			higher_layers.push_back(parseVssComponentSpec(server_val));
		}

		std::vector<VssComponentSpec> all_layers = higher_layers;
		if (effective_use & kUseValueClient)
		{
			all_layers.push_back(parseVssComponentSpec(client_val));
		}

		VssComponentSpec resolved;

		// Exclusions from any active layer override inclusions across all layers.
		for (size_t i = 0; i < all_layers.size(); ++i)
		{
			resolved.excluded_tokens.insert(
				all_layers[i].excluded_tokens.begin(),
				all_layers[i].excluded_tokens.end());
		}

		// Server/group explicit default=0 or all=0 forbids the client from turning them back on.
		bool higher_forbids_default = false;
		bool higher_forbids_all = false;
		for (size_t i = 0; i < higher_layers.size(); ++i)
		{
			if (higher_layers[i].has_default && !higher_layers[i].select_default)
			{
				higher_forbids_default = true;
			}
			if (higher_layers[i].has_all && !higher_layers[i].all)
			{
				higher_forbids_all = true;
			}
		}

		for (size_t i = 0; i < all_layers.size(); ++i)
		{
			if (all_layers[i].has_all)
			{
				resolved.has_all = true;
				if (all_layers[i].all && !higher_forbids_all)
				{
					resolved.all = true;
				}
			}
			if (all_layers[i].has_default)
			{
				resolved.has_default = true;
				if (all_layers[i].select_default && !higher_forbids_default)
				{
					resolved.select_default = true;
				}
			}

			for (size_t j = 0; j < all_layers[i].includes.size(); ++j)
			{
				const VssComponentRule& rule = all_layers[i].includes[j];
				if (isWriterExcluded(resolved.excluded_tokens, rule.writer))
				{
					continue;
				}
				if (std::find(resolved.includes.begin(), resolved.includes.end(), rule) == resolved.includes.end())
				{
					resolved.includes.push_back(rule);
				}
			}
		}

		if (higher_forbids_default)
		{
			resolved.has_default = true;
			resolved.select_default = false;
		}
		if (higher_forbids_all)
		{
			resolved.has_all = true;
			resolved.all = false;
		}

		return resolved;
	}

	// Convenience wrapper returning the resolved '&'-delimited vss_select_components string.
	inline std::string resolveVssComponentString(
		int use,
		bool allow_component_config,
		const std::string& group_val,
		const std::string& server_val,
		const std::string& client_val)
	{
		return serializeVssComponentSpec(
			resolveVssComponentSpec(use, allow_component_config, group_val, server_val, client_val));
	}
}

