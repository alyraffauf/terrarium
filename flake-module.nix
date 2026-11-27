{ ... }:
{
  flake.lib.vendorHash = "sha256-R8oXaSt9/FmswvVrGBPXNFhOrCzu7tbZGGDZj7xEn4A=";
  systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
  perSystem = { pkgs, self', ... }: {
    packages.default = self'.packages.capensis;
    devShells.default = pkgs.mkShell {
      packages = [ pkgs.go pkgs.sqlc pkgs.kubernetes-helm ];
      CGO_ENABLED = "0";
    };
  };
}
