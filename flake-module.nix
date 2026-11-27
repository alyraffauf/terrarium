{ ... }:
{
  flake.lib.vendorHash = "sha256-fCnCfcfyXY18o1bArZyKbL48iLcoAv6B6Q5FrZuD4hI=";
  systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
  perSystem = { pkgs, self', ... }: {
    packages.default = self'.packages.capensis;
    devShells.default = pkgs.mkShell {
      packages = [ pkgs.go pkgs.sqlc pkgs.kubernetes-helm ];
      CGO_ENABLED = "0";
    };
  };
}
