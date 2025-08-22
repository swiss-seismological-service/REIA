"""Plugin system for extending REIA CLI with additional commands."""

import logging
from abc import ABC, abstractmethod
from importlib.metadata import entry_points
from typing import Dict, List

import typer

logger = logging.getLogger(__name__)


class REIAPlugin(ABC):
    """Base class for REIA CLI plugins.

    Plugins must implement this interface to extend the REIA CLI
    with additional commands and functionality.
    """

    @abstractmethod
    def get_name(self) -> str:
        """Return the plugin name."""
        pass

    @abstractmethod
    def get_command_groups(self) -> Dict[str, typer.Typer]:
        """Return a dictionary of command groups to add to the CLI.

        Returns:
            Dict mapping group name to Typer instance
        """
        pass

    def get_commands(self) -> Dict[str, callable]:
        """Return a dictionary of individual commands to add to the main app.

        Returns:
            Dict mapping command name to function
        """
        return {}

    def initialize(self, app: typer.Typer) -> None:
        """Optional initialization hook called after plugin is loaded.

        Args:
            app: The main REIA Typer application
        """
        pass


class PluginManager:
    """Manages discovery and loading of REIA CLI plugins."""

    def __init__(self):
        self.plugins: List[REIAPlugin] = []
        self._loaded = False

    def discover_plugins(self) -> List[REIAPlugin]:
        """Discover and load all registered REIA plugins.

        Returns:
            List of loaded plugin instances
        """
        if self._loaded:
            return self.plugins

        discovered_plugins = []

        # Discover plugins via entry points
        eps = entry_points()

        # Get REIA plugins
        plugin_eps = eps.select(group='reia.plugins')

        for ep in plugin_eps:
            try:
                logger.debug(f"Loading plugin: {ep.name}")
                plugin_class = ep.load()

                # Instantiate the plugin
                if isinstance(
                        plugin_class,
                        type) and issubclass(
                        plugin_class,
                        REIAPlugin):
                    plugin = plugin_class()
                    discovered_plugins.append(plugin)
                    logger.debug(f"Loaded plugin: {plugin.get_name()}")
                else:
                    logger.warning(
                        f"Plugin {ep.name} does not implement "
                        f"REIAPlugin interface")

            except Exception as e:
                logger.error(f"Failed to load plugin {ep.name}: {e}")

        self.plugins = discovered_plugins
        self._loaded = True
        return discovered_plugins

    def register_plugins(self, app: typer.Typer) -> None:
        """Register all discovered plugins with the main CLI app.

        Args:
            app: The main REIA Typer application
        """
        plugins = self.discover_plugins()

        for plugin in plugins:
            try:
                # Register command groups
                command_groups = plugin.get_command_groups()
                for name, group in command_groups.items():
                    logger.debug(
                        f"Registering command group '{name}' "
                        f"from plugin {plugin.get_name()}")
                    app.add_typer(group, name=name)

                # Register individual commands
                commands = plugin.get_commands()
                for name, command in commands.items():
                    logger.debug(
                        f"Registering command '{name}' "
                        f"from plugin {plugin.get_name()}")
                    app.command(name=name)(command)

                # Call initialization hook
                plugin.initialize(app)

            except Exception as e:
                logger.error(
                    f"Failed to register plugin {plugin.get_name()}: {e}")


# Global plugin manager instance
plugin_manager = PluginManager()
