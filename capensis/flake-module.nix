{ inputs, ... }:
{
  perSystem = { pkgs, self', ... }: {
    packages.capensis = pkgs.buildGoModule {
      pname = "capensis";
      version = "0.1.0";
      src = inputs.self;
      vendorHash = inputs.self.lib.vendorHash;
      subPackages = [ "cmd/capensis" ];
      env.CGO_ENABLED = 0;
      ldflags = [ "-s" "-w" ];
      checkPhase = ''
        runHook preCheck
        go test ./capensis/... ./palustre/...
        runHook postCheck
      '';
      meta = {
        description = "Maintain moderation lists for accounts hosted on selected PDSes";
        mainProgram = "capensis";
        license = pkgs.lib.licenses.mit;
        platforms = pkgs.lib.platforms.unix;
      };
    };
    checks.capensis = self'.packages.capensis;
    devShells.capensis = self'.devShells.default;
  };
}
