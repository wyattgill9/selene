{ inputs, ... }:
{
  perSystem =
    {
      pkgs,
      craneLib,
      ...
    }:
    let
      src = craneLib.cleanCargoSource ../.;

      commonArgs = {
        inherit src;
        strictDeps = true;

        nativeBuildInputs = with pkgs; [
          pkg-config
        ];

        # Darwin SDK frameworks come from the stdenv now — no apple_sdk stubs.
        buildInputs = with pkgs; [
          openssl
        ];
      };

      cargoArtifacts = craneLib.buildDepsOnly commonArgs;

      mkCrate =
        pname:
        craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts pname;
            cargoExtraArgs = "-p ${pname}";
          }
        );
    in
    {
      packages = {
        cli = mkCrate "cli";
        default = mkCrate "cli";
      };

      # `nix flake check` runs these plus treefmt (added by the treefmt-nix module).
      checks = {
        clippy = craneLib.cargoClippy (
          commonArgs
          // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          }
        );

        test = craneLib.cargoNextest (
          commonArgs
          // {
            inherit cargoArtifacts;
            partitions = 1;
            partitionType = "count";
          }
        );
      };

      _module.args = {
        inherit commonArgs cargoArtifacts src;
      };
    };
}
