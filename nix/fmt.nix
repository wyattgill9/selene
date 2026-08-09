{ ... }:
{
  perSystem =
    { ... }:
    {
      treefmt.config = {
        projectRootFile = "flake.nix";

        programs = {
          rustfmt.enable = true;
          nixfmt.enable = true;
          taplo.enable = true;
        };
      };
    };
}
