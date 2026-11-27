{ inputs, ... }:
{
  perSystem = { pkgs, self', ... }: {
    packages.trifolium = pkgs.buildGoModule {
      pname = "trifolium";
      version = "0.1.0";
      src = inputs.self;
      vendorHash = inputs.self.lib.vendorHash;
      subPackages = [ "cmd/trifolium" ];
      env.CGO_ENABLED = 0;
      ldflags = [ "-s" "-w" ];
      checkPhase = ''
        runHook preCheck
        go test ./cmd/trifolium/... ./palustre/...
        runHook postCheck
      '';
      meta = {
        description = "Maintain a moderation list based on follower overlap";
        mainProgram = "trifolium";
        license = pkgs.lib.licenses.mit;
        platforms = pkgs.lib.platforms.unix;
      };
    };
    checks.trifolium = self'.packages.trifolium;
    devShells.trifolium = self'.devShells.default;
  };
}
